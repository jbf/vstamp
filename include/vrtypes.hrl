%%% Macros
-define(WAIT, 7000).
-define(PING, 5000).
-define(TIMEOUT, 1000).

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
-type client_table() :: map:map(token(), client_req_trace()).

-record(state, { index :: replica_num()
               , config :: config()
               , view :: view_num()
               , commit :: commit_num()
               , op_num = -1 :: op_num()
               , req_trace = #{} :: client_table()
               , log = [] :: log()
               }).

-define(cluster_size(X), length(X)).
