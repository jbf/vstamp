-module(viewstamp_unit_tests).

-include_lib("eunit/include/eunit.hrl").

config() -> [{foo, node()},
             {bar, node()},
             {baz, node()}].

me_test_() ->
  N = node(),
  [ ?_assertEqual({foo, N}, vstamp_replica:me(1, config()))
  , ?_assertEqual({baz, N}, vstamp_replica:me(3, config()))
  , ?_assertError(function_clause, vstamp_replica:me(4, config())) ].

not_me_test_() ->
  List = config(),
  NotFoo = List -- [{foo, node()}],
  NotBar = List -- [{bar, node()}],
  [ ?_assertEqual(NotFoo, vstamp_replica:not_me(1, config()))
  , ?_assertEqual(NotBar, vstamp_replica:not_me(2, config()))
  , ?_assertError(function_clause, vstamp_replica:not_me(4, config())) ].

find_by_name_test_() ->
  N = node(),
  [ ?_assertEqual({1, {foo, N}}, vstamp_replica:find_by_name(foo, config()))
  , ?_assertEqual({3, {baz, N}}, vstamp_replica:find_by_name(baz, config()))
  , ?_assertEqual(not_found, vstamp_replica:find_by_name(quux, config())) ].

submajority_test_() ->
  [ ?_assertEqual(1, vstamp_replica:submajority(config()))
  , ?_assertEqual(2, vstamp_replica:submajority([a,b,c,d]))
  , ?_assertEqual(0, vstamp_replica:submajority([a])) ].

post_three_ops_test_() ->
  {setup,
    with_nodes([foo, bar, baz]),
    fun helpers:stop_cluster/1,
    fun([Primary, Secondary, Tertiary|_Nodes]) ->
        {ok, Tok} = vstamp_replica:get_client_token(Primary),
        vstamp_replica:request(Primary, Tok, 1, foo),
        vstamp_replica:request(Primary, Tok, 2, bar),
        vstamp_replica:request(Primary, Tok, 10, baz),
        Log = [{2, {'REQUEST', '_', 10, baz}},
               {1, {'REQUEST', '_', 2, bar}},
               {0, {'REQUEST', '_', 1, foo}}],
        [ assertLog(Log, Primary),
          assertLog(Log, Secondary),
          assertLog(Log, Tertiary) ]
    end
  }.

%% Test that the log comparison fails on unequal lines
assert_log_test() ->
  lists:foreach(fun({Line, Fun}) ->
                    try
                      Fun(),
                      erlang:error({failed, Line})
                    catch
                        error:{assertEqual_failed, _} -> ok
                    end
                end,
                assertReqLines([{1, {'OP', asf, 1, op1}}], [{2, {'OP2', asf2, 3, op2}}])).

with_nodes(Nodes) ->
  fun() ->
    Res = helpers:start_local(Nodes),
    lists:map(fun(E) -> element(3, E) end, Res)
  end.

assertLog(Expected, Node) ->
  {ok, NodeLog} = vstamp_replica:get_log(Node),
  assertReqLines(Expected, NodeLog).

assertReqLines([], []) -> [];
assertReqLines([H], []) ->
  [ ?_assertEqual(H, []) ];
assertReqLines([], [H]) ->
  [ ?_assertEqual([], H) ];
assertReqLines([Expected|Tail], [Actual|More]) ->
  assertLine(Expected, Actual) ++ assertReqLines(Tail, More).

assertLine({Nr, {Op, _Ref, Seq, Val}}, {Nr2, {Op2, _SomeRef, Seq2, Val2}}) ->
  [ ?_assertEqual(Nr, Nr2)
  , ?_assertEqual(Op, Op2)
  , ?_assertEqual(Seq, Seq2)
  , ?_assertEqual(Val, Val2)
  ].
