-module(vstamp_replica).

-behavior(gen_server).

-include("vrtypes.hrl").

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
  TimerRef = make_ref(),
  {ok, Timer} = timer:send_interval(?PING, {TimerRef, ?PING_MSG}),
  Commit = case Index =:= 1 of
             true -> 0;
             _ -> undefined
           end,
  {ok, #state{index=Index, config=Config, view=View,
              commit=Commit, primary_timer={TimerRef, Timer}}}.

handle_call(get_client_token, _From, State = #state{req_trace=Trace}) ->
  Ref = make_ref(),
  {reply, {ok, Ref},
   State#state{req_trace=maps:put(Ref, {-1, undefined}, Trace)}};
handle_call(get_state, _From, State) ->
  {reply, {ok, State}, State};
handle_call(get_log, _From, State = #state{log=Log}) ->
  {reply, {ok, Log}, State};
handle_call(R = {'REQUEST', Token, _ReqNum, _Op}, _From, State) ->
  case vstamp_config_lib:is_primary(State) of
    false -> {reply, {error, not_primary}, State}; %% paper says drop, why?
    true ->
        Clients = State#state.req_trace,
        case maps:find(Token, Clients) of
          error -> {reply, {error, token_not_valid}, State, 0};
          {ok, T = {_Max, _Res}} ->
            handle_req_with_client(R, T, State)
        end
  end;
handle_call(Call, _From, State) ->
  lager:notice("Unknown call: ~p~n", [Call]),
  {reply, {ok, Call}, State}.

handle_req_with_client({'REQUEST', _T, ReqNum, _Op}, {Max, _R}, State)
  when ReqNum < Max -> {reply, {error, {seen_req, Max}}, State};
handle_req_with_client({'REQUEST', _T, ReqNum, _Op}, {Max, Result}, State)
  when ReqNum =:= Max -> {reply, {ok, Result}, State};
handle_req_with_client(R = {'REQUEST', _T, ReqNum, _Op}, {Max, _Result}, State)
  when ReqNum > Max -> do_handle_request(R, State).

do_handle_request(R = {'REQUEST', Token, ReqNum, _Op}, State) ->
  NewOpNum = State#state.op_num + 1,
  NewReqTrace = {ReqNum, no_result_yet},
  NewLog = [{NewOpNum, R} | State#state.log],
  NewTraces = maps:put(Token, NewReqTrace, State#state.req_trace),
  NewState = State#state{req_trace=NewTraces, op_num=NewOpNum, log=NewLog},
  case do_or_timeout(R, NewState) of
    {abort, AbortState} ->
      {reply, {error, {timeout, R}}, AbortState};
    {committed, CommitState, Res} ->
      FinalTrace = maps:put(Token, {ReqNum, Res}, State#state.req_trace),
      {reply, {ok, R, Res}, CommitState#state{req_trace=FinalTrace}}
  end.

do_or_timeout(R, S = #state{config=Config}) ->
  View = S#state.view,
  OpNum = S#state.op_num,
  CurrentCommit = S#state.commit,
  Index = S#state.index,
  send(vstamp_config_lib:not_me(Index, Config), {'PREPARE', Index, View, R, OpNum, CurrentCommit}),
  do_or_timeout(R, S, []).

do_or_timeout(R, S = #state{config=Config}, Replies) ->
  View = S#state.view,
  OpNum = S#state.op_num,
  NumReplies = length(Replies),
  Submajority = vstamp_config_lib:submajority(Config),
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
  lager:info("COMMIT: ~p, STATE: ~p~n", [R, S]),
  {committed, S#state{commit=S#state.op_num}, {'op_committed:', Op}}.

send(Nodes, Msg) ->
      [gen_server:abcast([Node], Name, Msg) || {Name, Node} <- Nodes].

handle_cast(Msg = {'PREPARE', From, View, _R, _OpNum, Commit}, State) ->
  State0 = maybe_commit(From, View, Commit, State),
  State1 = maybe_accept_prepare(Msg, State0),
  {noreply, State1};

handle_cast({'COMMIT', From, View, Commit}, State) ->
  NewState = maybe_commit(From, View, Commit, State),
  {noreply, NewState}.

maybe_commit(From, View, Commit, State) ->
  MyView = State#state.view,
  case View of
    N when N > MyView -> %% initiate recovery
      State;
    N when N < MyView -> %% maybe tell initiator view has changed?
      State;
    N when N =:= MyView ->
      case vstamp_config_lib:is_primary_in_view(From, View, State#state.config) of
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
  case vstamp_config_lib:is_current_view(View, MyView) andalso
       vstamp_config_lib:is_primary_in_view(From, View, State#state.config) andalso
       ((TopNum =:= OpNum -1) orelse (TopNum =:= ok)) of
    true ->
      send1(From, Config, {'PREPARE_OK', View, OpNum, State#state.index}),
      State#state{op_num=OpNum, log=[{OpNum, R} | State#state.log]};
    false -> State
  end.

send1(To, Config, Msg) ->
  NameNode = vstamp_config_lib:find_by_index(To, Config),
  NameNode ! Msg.

handle_info({'PREPARE_OK', _View, _OpNum, _Other} = Msg, State) ->
  lager:debug("Delayed message: ~p~n", [Msg]),
  {noreply, State};

handle_info({Ref, ?PING_MSG}, State = #state{index=Index, config=Config,
                                             view=View, primary_timer={Ref, _}}) ->
  Committed = State#state.commit,
  case vstamp_config_lib:is_primary(State) of
    %% No op before timeout. I am the primary, send out periodic heartbeat.
    true -> send(vstamp_config_lib:not_me(Index, Config), {'COMMIT', Index, View, Committed});
    %% I am not the primary.
    false -> ok
  end,
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%% Helpers
do_start(Name, Args, StartFun) ->
  case vstamp_config_lib:find_by_name(Name, get_config(Args)) of
    {Index, {Name, Host}} -> vstamp_config_lib:assert_host(Host),
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
