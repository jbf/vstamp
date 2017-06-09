-module(vstamp_replica).

-behavior(gen_server).

%-ifdef(TEST).
-compile(export_all).
%-endif.

%%% API
-export([ get_client_token/1
        , request/4
        ]).

%%% Testing
-export([ start/2
        , get_state/1
        , get_log/1
        ]).

%%% gen_server
-export([ init/1
        , code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-include("../include/vrtypes.hrl").

-type replica() :: {name(), host()}.
-type name() :: atom().
-type host() :: atom().
-type config() :: [ replica() ].
-type replica_num() :: non_neg_integer().
-type view_num() :: non_neg_integer().
-type op_num() :: non_neg_integer() | -1.
-type log() :: list().
-type commit_num() :: non_neg_integer() | undefined.
-type client_req_trace() :: { op_num() | -1, result() | undefined }.
-type token() :: reference().
-type result() :: any().
-type client_table() :: map(token(), client_req_trace()).

%%% Macros
-define(cluster_size(T), tuple_size(T)).

-define(WAIT, 7000).
-define(PING, 5000).
-define(TIMEOUT, 1000).

-record(state, { index :: replica_num()
               , config :: config()
               , view :: view_num()
               , commit :: commit_num()
               , op_num = -1 :: op_num()
               , req_trace = #{} :: client_table()
               , log = [] :: log()
               }).

%%% API
get_client_token(Replica) ->
  gen_server:call(Replica, get_client_token).

request(Replica, Token, ReqNum, Op) ->
  gen_server:call(Replica, {'REQUEST', Token, ReqNum, Op}).

%%% Testing
% vstamp_replica:start(name, [{config, {{name, SName@SHost}}]).
start(Name, Args) ->
  do_start(Name, Args, fun gen_server:start/4).

get_state(Replica) ->
  gen_server:call(Replica, get_state).

get_log(Replica) ->
  gen_server:call(Replica, get_log).

%%% gen_server
init(Args) ->
  Index = get_arg(index, Args),
  View = 1,
  Config = get_arg(config, Args),
  Timeout = 0,
  Commit = case index =:= 1 of
             true -> 0;
             _ -> undefined
           end,
  {ok, #state{index=Index, config=Config, view=View, commit=Commit}, Timeout}.

handle_call(get_client_token, _From, State = #state{req_trace=Trace}) ->
  Ref = make_ref(),
  {reply,
   {ok, Ref},
   State#state{req_trace=maps:put(Ref, {-1, undefined}, Trace)},
   get_timeout(State)};
handle_call(get_state, _From, State) ->
  {reply, {ok, State}, State, get_timeout(State)};
handle_call(get_log, _From, State = #state{log=Log}) ->
  {reply, {ok, Log}, State, get_timeout(State)};
handle_call(R = {'REQUEST', Token, _ReqNum, _Op}, _From, State) ->
  case is_primary(State) of
    false -> {reply, {error, not_primary}, State, ?WAIT}; %% paper says drop, why?
    true ->
        Clients = State#state.req_trace,
        case maps:find(Token, Clients) of
          error -> {reply, {error, token_not_valid}, State, 0};
          {ok, T = {_Max, _Res}} ->
            handle_req_with_client(R, T, State)
        end
  end;
handle_call(Call, _From, State) ->
  io:format("Unknown call: ~p~n", [Call]),
  {reply, {ok, Call}, State, get_timeout(State)}.

get_timeout(State) ->
  case is_primary(State) of
      true -> 0;
      false -> ?WAIT
  end.


handle_req_with_client(R = {'REQUEST', _Token, ReqNum, _Op},
                       {Max, Result},
                       State) ->
  case ReqNum of
    N when N < Max -> {reply, {error, {seen_req, Max}}, State, 0};
    N when N =:= Max -> {reply, {ok, Result}, State, 0};
    N when N > Max -> do_handle_request(R, State)
  end.

do_handle_request(R = {'REQUEST', Token, ReqNum, _Op}, State) ->
  NewOpNum = State#state.op_num + 1,
  NewReqTrace = {ReqNum, no_result_yet},
  NewLog = [{NewOpNum, R} | State#state.log],
  NewTraces = maps:put(Token, NewReqTrace, State#state.req_trace),
  NewState = State#state{req_trace=NewTraces, op_num=NewOpNum, log=NewLog},
  case do_or_timeout(R, NewState) of
    {timeout, AbortState} ->
      {reply, {error, {timeout, R}}, AbortState, ?PING};
    {committed, CommitState, Res} ->
      FinalTrace = maps:put(Token, {ReqNum, Res}, State#state.req_trace),
      {reply, {ok, R, Res}, CommitState#state{req_trace=FinalTrace}, ?PING}
  end.

do_or_timeout(R, S = #state{config=Config}) ->
  View = S#state.view,
  OpNum = S#state.op_num,
  CurrentCommit = S#state.commit,
  Index = S#state.index,
  send(not_me(Index, Config), {'PREPARE', Index, View, R, OpNum, CurrentCommit}),
  do_or_timeout(R, S, []).

do_or_timeout(R, S = #state{config=Config}, Replies) ->
  View = S#state.view,
  OpNum = S#state.op_num,
  NumReplies = length(Replies),
  Submajority = submajority(Config),
  case NumReplies of
    N when N < Submajority ->
      receive
        Reply = {'PREPARE_OK', View, OpNum, _Other} ->
          do_or_timeout(R, S, [Reply | Replies])
      after
        ?TIMEOUT ->
          {abort, S}
      end;
    N when N < (?cluster_size(Config) - 1) ->
      receive
        Reply = {'PREPARE_OK', View, OpNum, _Other} ->
          do_or_timeout(R, S, [Reply | Replies])
      after
        5 -> do_commit(R, S)
      end;
    _N  -> do_commit(R, S)
  end.

do_commit(R = {_R, _T, _N, Op}, S) ->
  io:format("COMMIT: ~p, STATE: ~p~n", [R, S]),
  {committed, S#state{commit=S#state.op_num}, {'op_committed:', Op}}.

send(Nodes, Msg) ->
      [gen_server:abcast([Node], Name, Msg) || {Name, Node} <- Nodes].

handle_cast(Msg = {'PREPARE', From, View, _R, _OpNum, Commit}, State) ->
  State0 = maybe_commit(From, View, Commit, State),
  State1 = maybe_accept_prepare(Msg, State0),
  Timeout = calculate_timeout(State),
  {noreply, State1, Timeout};

handle_cast({'COMMIT', From, View, Commit}, State) ->
  NewState = maybe_commit(From, View, Commit, State),
  {noreply, NewState, ?WAIT}.

maybe_commit(From, View, Commit, State) ->
  MyView = State#state.view,
  case View of
    N when N > MyView -> %% initiate recovery
      State;
    N when N < MyView -> %% maybe tell initiator view has changed?
      State;
    N when N =:= MyView ->
      case is_primary_in_view(From, View, State#state.config) of
        true ->  commit_committed(Commit, State);
        false -> State %% maybe tell?
      end
  end.

commit_committed(To, State = #state{commit=Commit}) when To =:= Commit ->
  State;
commit_committed(_, State = #state{log=[]}) -> %% Initiate recovery
  State;
commit_committed(To, State = #state{log=[{Num, _}|_]}) when To > Num ->
  %% Initiate recovery
  State;
commit_committed(To, State = #state{log=[{Num, _}|_]}) when To =< Num ->
  State#state{commit=To}.

maybe_accept_prepare({'PREPARE', From, View, R, OpNum, _Commit}, State) ->
  MyLog = State#state.log,
  TopNum = case MyLog of
             [] -> ok;
             [{Num, _}|_] -> Num
           end,
  MyView = State#state.view,
  Config = State#state.config,
  case is_current_view(View, MyView) andalso
       is_primary_in_view(From, View, State#state.config) andalso
       ((TopNum =:= OpNum -1) orelse (TopNum =:= ok)) of
    true ->
      send1(From, Config, {'PREPARE_OK', View, OpNum, State#state.index}),
      State#state{op_num=OpNum, log=[{OpNum, R} | State#state.log]};
    false -> State
  end.

send1(To, Config, Msg) ->
  NameNode = find_by_index(To, Config),
  NameNode ! Msg.

handle_info({'PREPARE_OK', _View, _OpNum, _Other} = Msg, State) ->
  io:format("Delayed message: ~p~n", [Msg]),
  Timeout = calculate_timeout(State),
  {noreply, State, Timeout};

handle_info(timeout, State = #state{index=Index, config=Config, view=View}) ->
  Timeout = calculate_timeout(State),
  Committed = State#state.commit,
  case is_primary(State) of
    %% No op before timeout. I am the primary, send out periodic heartbeat.
    true -> send(not_me(Index, Config), {'COMMIT', Index, View, Committed});
    %% I am not the primary. TODO: initiate view change here.
    false -> ok
  end,
  {noreply, State, Timeout}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%% Helpers
do_start(Name, Args, StartFun) ->
  case find_by_name(Name, get_config(Args)) of
    {Index, {Name, Host}} -> assert_host(Host),
                             StartFun({local, Name}, ?MODULE,
                                      [{index, Index}] ++ Args, []);
    _ -> {error, host_not_in_config}
  end.

get_config(Args) ->
  get_arg(config, Args).

get_arg(Arg, Args) ->
  case lists:keyfind(Arg, 1, Args) of
    false -> undefined;
    {Arg, Res} -> Res
  end.

%% Config tools
find_by_name(Name, Config) ->
  case lists:keyfind(Name, 1, Config) of
    false -> not_found;
    Res when is_tuple(Res) -> {get_index(Res, Config), Res}
  end.

get_index(Conf, Config) ->
  get_index(1, Conf, Config).

get_index(Res, Conf, [Conf|_]) -> Res;
get_index(_Res, _Conf, []) -> not_found;
get_index(Res, Conf, [_H|T]) -> get_index(Res+1, Conf, T).


find_by_index(1, [E|_]) -> E;
find_by_index(I, [_|T]) ->
  find_by_index(I-1,T).

not_me(Index, Config) ->
  {Me, _} = me(Index, Config),
  lists:keydelete(Me, 1, Config).

me(Index, Config) ->
    lists:nth(Index, Config).

assert_host(Host) ->
  Host = node().

view_to_index(View, Config) ->
  View rem length(Config).

is_primary(#state{index=Index, view=View, config=Config}) ->
  is_primary_in_view(Index, View, Config).

is_primary_in_view(Index, View, Config) ->
  view_to_index(View, Config) =:= Index.

submajority(Config) ->
  length(Config) div 2.

calculate_timeout(State) ->
  case is_primary(State) of
    true -> ?PING;
    false -> ?WAIT
  end.

is_current_view(View1, View2) when View1 =:= View2 -> true;
is_current_view(_View1, _View2) -> false.
