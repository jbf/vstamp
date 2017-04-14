-module(helpers).

-compile(export_all).

setup_cluster(L) ->
  LL = lists:reverse(L),
  Spec = cluster_spec(LL, {}),
  {ok, [Pid || {ok, Pid} <-
               [vstamp_replica:start(Name, [{config, Spec}]) || Name <- L]]}.

cluster_spec([], Res) -> Res;
cluster_spec([H|T], Acc) ->
  cluster_spec(T, erlang:insert_element(1, Acc, {H, node()})).

stop_cluster(Pids) ->
  [ exit(Pid, stop) || Pid <- Pids ].
