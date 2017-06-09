-module(helpers).

-export([ start_node/3
        %, start_nodes/1
        , start_local/1
        , stop_node/1]).

-compile(export_all).

-include("../include/vrtypes.hrl").

start_local(VRNames) ->
  Node = node(),
  Config = lists:map(fun(Name) -> {Name, Node} end, VRNames),
  lists:map(fun(Conf) -> do_start(Conf, Config) end, Config).

do_start(Conf, Config) ->
  Node = node(),
  case Conf of
    {Name, Node} -> {ok, Pid} = vstamp_replica:start(Name, [{config, Config}]),
                    {Name, Node, Pid};
    _ -> ok % TODO: start remote node, not yet implemented
  end.

stop_cluster(Pids) ->
  [ exit(Pid, stop) || Pid <- Pids ].
