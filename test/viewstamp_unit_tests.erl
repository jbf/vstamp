-module(viewstamp_unit_tests).

-include_lib("eunit/include/eunit.hrl").

config() -> {{foo, node()},
             {bar, node()},
             {baz, node()}}.

me_test_() ->
  N = node(),
  [ ?_assertEqual({foo, N}, vstamp_replica:me(0, config()))
  , ?_assertEqual({baz, N}, vstamp_replica:me(2, config()))
  , ?_assertError(badarg, vstamp_replica:me(3, config())) ].

not_me_test_() ->
  List = [{foo, node()}, {bar, node()}, {baz, node()}],
  NotFoo = List -- [{foo, node()}],
  NotBar = List -- [{bar, node()}],
  [ ?_assertEqual(NotFoo, vstamp_replica:not_me(0, config()))
  , ?_assertEqual(NotBar, vstamp_replica:not_me(1, config()))
  , ?_assertEqual(List, vstamp_replica:not_me(3, config())) ].

find_by_name_test_() ->
  N = node(),
  [ ?_assertEqual({0, {foo, N}},
                  vstamp_replica:find_by_name(foo, config()))
  , ?_assertEqual({2, {baz, N}},
                  vstamp_replica:find_by_name(baz, config()))
  , ?_assertEqual(not_found, vstamp_replica:find_by_name(quux, config())) ].

submajority_test_() ->
  [ ?_assertEqual(1, vstamp_replica:submajority(config()))
  , ?_assertEqual(2, vstamp_replica:submajority({a,b,c,d}))
  , ?_assertEqual(0, vstamp_replica:submajority({a})) ].
