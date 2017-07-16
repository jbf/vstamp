-module(vstamp_config_lib).

-include("vrtypes.hrl").

-export([ find_by_name/2
        , get_index/2
        , find_by_index/2
        , not_me/2
        , me/2
        , view_to_index/2
        , is_primary/1
        , is_primary_in_view/3
        , submajority/1
        , is_current_view/2
        , assert_host/1
        ]).

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

is_current_view(View1, View2) when View1 =:= View2 -> true;
is_current_view(_View1, _View2) -> false.
