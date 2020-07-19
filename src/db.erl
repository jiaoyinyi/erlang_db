%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2020, <COMPANY>
%%% @doc 数据库操作接口
%%%
%%% @end
%%% Created : 18. 7月 2020 17:24
%%%-------------------------------------------------------------------
-module(db).
-author("jiaoyinyi").

%% API
-export([
    query/2, query/3, query/4, query/5,
    pool_query/1, pool_query/2, pool_query/3, pool_query/4,
    execute/3, execute/4, execute/5,
    pool_execute/2, pool_execute/3, pool_execute/4,
    prepare/2, prepare/3, unprepare/2,
    warning_count/1, affected_rows/1, autocommit/1, insert_id/1,
    encode/2, in_transaction/1,
    pool_transaction/1, pool_transaction/2, pool_transaction/3,
    change_user/3, change_user/4, reset_connection/1, is_connected/1,
    get_worker/0, put_worker/1,
    get_conn/1
]).

-define(DB_CALL(Cli_Pid, Call), gen_server:call(Cli_Pid, Call)).
-define(POOL_NAME, db_pool).

%% MySQL error with the codes and message returned from the server.
-type server_reason() :: {Code :: integer(), SQLState :: binary() | undefined,
    Message :: binary()}.

-type column_names() :: [binary()].
-type row() :: [term()].
-type rows() :: [row()].

-type query_filtermap_fun() :: fun((row()) -> query_filtermap_res())
                            | fun((column_names(), row()) -> query_filtermap_res()).
-type query_filtermap_res() :: boolean()
                            | {true, term()}.

-type query_result() :: ok
                        | {ok, column_names(), rows()}
                        | {ok, [{column_names(), rows()}, ...]}
                        | {error, server_reason()}.

-spec query(Conn, Query) -> Result
    when Conn :: mysql:contcion(),
    Query :: iodata(),
    Result :: query_result() | {error, fail}.
query(Conn, Query) ->
    case catch mysql:query(Conn, Query) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec query(Conn, Query, Args | FilterMap | Timeout) -> Result
    when Conn :: mysql:contcion(),
    Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result() | {error, fail}.
query(Conn, Query, Args) when is_list(Args) ->
    case catch mysql:query(Conn, Query, Args) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
query(Conn, Query, FilterMap) when is_function(FilterMap, 1) orelse is_function(FilterMap, 2) ->
    case catch mysql:query(Conn, Query, FilterMap) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
query(Conn, Query, Timeout) when is_integer(Timeout) orelse Timeout == infinity ->
    case catch mysql:query(Conn, Query, Timeout) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec query(Conn, Query, Args, Timeout) -> Result
    when Conn :: mysql:contcion(),
    Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    Result :: query_result() | {error, fail};
    (Conn, Query, FilterMap, Timeout) -> Result
    when Conn :: mysql:contcion(),
    Query :: iodata(),
    Timeout :: timeout(),
    FilterMap :: query_filtermap_fun(),
    Result :: query_result() | {error, fail};
    (Conn, Query, Args, FilterMap) -> Result
    when Conn :: mysql:contcion(),
    Query :: iodata(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result() | {error, fail}.
query(Conn, Query, Args, Timeout) when is_list(Args) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch mysql:query(Conn, Query, Args, Timeout) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
query(Conn, Query, FilterMap, Timeout) when (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch mysql:query(Conn, Query, FilterMap, Timeout) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
query(Conn, Query, Args, FilterMap) when is_list(Args) andalso (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) ->
    case catch mysql:query(Conn, Query, Args, FilterMap) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec query(Conn, Query, Args, FilterMap, Timeout) -> Result
    when Conn :: mysql:contcion(),
    Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result() | {error, fail}.
query(Conn, Query, Args, FilterMap, Timeout) when is_list(Args) andalso (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch mysql:query(Conn, Query, Args, FilterMap, Timeout) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec pool_query(Query) -> Result
    when Query :: iodata(),
    Result :: query_result() | {error, fail}.
pool_query(Query) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec pool_query(Query, Args | FilterMap | Timeout) -> Result
    when Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result() | {error, fail}.
pool_query(Query, Args) when is_list(Args) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Args}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
pool_query(Query, FilterMap) when is_function(FilterMap, 1) orelse is_function(FilterMap, 2) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, FilterMap}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
pool_query(Query, Timeout) when is_integer(Timeout) orelse Timeout == infinity ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec pool_query(Query, Args, Timeout) -> Result
    when Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    Result :: query_result() | {error, fail};
    (Query, FilterMap, Timeout) -> Result
    when Query :: iodata(),
    Timeout :: timeout(),
    FilterMap :: query_filtermap_fun(),
    Result :: query_result() | {error, fail};
    (Query, Args, FilterMap) -> Result
    when Query :: iodata(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result() | {error, fail}.
pool_query(Query, Args, Timeout) when is_list(Args) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Args, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
pool_query(Query, FilterMap, Timeout) when (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, FilterMap, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
pool_query(Query, Args, FilterMap) when is_list(Args) andalso (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Args, FilterMap}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec pool_query(Query, Args, FilterMap, Timeout) -> Result
    when Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result() | {error, fail}.
pool_query(Query, Args, FilterMap, Timeout) when is_list(Args) andalso (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Args, FilterMap, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec execute(Conn, Ref, Args) -> Result
    when Conn :: mysql:contcion(),
    Ref :: atom() | integer(),
    Args :: [term()],
    Result :: query_result() | {error, fail}.
execute(Conn, Ref, Args) ->
    case catch mysql:execute(Conn, Ref, Args) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec execute(Conn, Ref, Args, FilterMap | Timeout) -> Result
    when Conn :: mysql:contcion(),
    Ref :: atom() | integer(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Timeout :: timeout(),
    Result :: query_result() | {error, fail}.
execute(Conn, Ref, Args, Timeout) when is_integer(Timeout) orelse Timeout == infinity ->
    case catch mysql:execute(Conn, Ref, Args, Timeout) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
execute(Conn, Ref, Args, FilterMap) when is_function(FilterMap, 1) orelse is_function(FilterMap, 2) ->
    case catch mysql:execute(Conn, Ref, Args, FilterMap) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec execute(Conn, Ref, Args, FilterMap, Timeout) -> Result
    when Conn :: mysql:contcion(),
    Ref :: atom() | integer(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Timeout :: timeout(),
    Result :: query_result() | {error, fail}.
execute(Conn, Ref, Args, FilterMap, Timeout) ->
    case catch mysql:execute(Conn, Ref, Args, FilterMap, Timeout) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec pool_execute(Ref, Args) -> Result
    when Ref :: atom() | integer(),
    Args :: [term()],
    Result :: query_result() | {error, fail}.
pool_execute(Ref, Args) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {execute, Ref, Args}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec pool_execute(Ref, Args, FilterMap | Timeout) -> Result
    when Ref :: atom() | integer(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Timeout :: timeout(),
    Result :: query_result() | {error, fail}.
pool_execute(Ref, Args, Timeout) when is_integer(Timeout) orelse Timeout == infinity ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {execute, Ref, Args, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
pool_execute(Ref, Args, FilterMap) when is_function(FilterMap, 1) orelse is_function(FilterMap, 2) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {execute, Ref, Args, FilterMap}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec pool_execute(Ref, Args, FilterMap, Timeout) -> Result
    when Ref :: atom() | integer(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Timeout :: timeout(),
    Result :: query_result() | {error, fail}.
pool_execute(Ref, Args, FilterMap, Timeout) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {execute, Ref, Args, FilterMap, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec prepare(Conn, Query) -> {ok, StatementId} | {error, Reason}
    when Conn :: mysql:contcion(),
    Query :: iodata(),
    StatementId :: integer(),
    Reason :: server_reason() | fail.
prepare(Conn, Query) ->
    case catch mysql:prepare(Conn, Query) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec prepare(Conn, Name, Query) -> {ok, Name} | {error, Reason}
    when Conn :: mysql:contcion(),
    Name :: atom(),
    Query :: iodata(),
    Reason :: server_reason() | fail.
prepare(Conn, Name, Query) ->
    case catch mysql:prepare(Conn, Name, Query) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec unprepare(Conn, StatementRef) -> ok | {error, Reason}
    when Conn :: mysql:contcion(),
    StatementRef :: atom() | integer(),
    Reason :: server_reason() | not_prepared | fail.
unprepare(Conn, StatementRef) ->
    case catch mysql:unprepare(Conn, StatementRef) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec warning_count(Conn :: mysql:connection()) -> {ok, integer()} | {error, fail}.
warning_count(Conn) ->
    case catch mysql:warning_count(Conn) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            {ok, Ret}
    end.

-spec affected_rows(Conn :: mysql:connection()) -> {ok, integer()} | {error, fail}.
affected_rows(Conn) ->
    case catch mysql:affected_rows(Conn) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            {ok, Ret}
    end.

-spec autocommit(Conn :: mysql:connection()) -> {ok, boolean()} | {error, fail}.
autocommit(Conn) ->
    case catch mysql:autocommit(Conn) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            {ok, Ret}
    end.

-spec insert_id(Conn :: mysql:connection()) -> {ok, integer()} | {error, fail}.
insert_id(Conn) ->
    case catch mysql:insert_id(Conn) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            {ok, Ret}
    end.

-spec encode(Conn :: mysql:connection(), term()) -> {ok, iodata()} | {error, fail}.
encode(Conn, Term) ->
    case catch mysql:encode(Conn, Term) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            {ok, Ret}
    end.

-spec in_transaction(Conn :: mysql:connection()) -> {ok, boolean()} | {error, fail}.
in_transaction(Conn) ->
    case catch mysql:in_transaction(Conn) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            {ok, Ret}
    end.

-spec pool_transaction(fun()) -> {ok, term()} | {error, term()}.
pool_transaction(Fun) when is_function(Fun, 1) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {transaction, Fun}) end) of
        {atomic, Term} ->
            {ok, Term};
        {aborted, Term} ->
            {error, Term};
        _Err ->
            {error, fail}
    end.

-spec pool_transaction(fun(), Retries) -> {ok, term()} | {error, term()} when Retries :: non_neg_integer() | infinity.
pool_transaction(Fun, Retries) when is_function(Fun, 1) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {transaction, Fun, Retries}) end) of
        {atomic, Term} ->
            {ok, Term};
        {aborted, Term} ->
            {error, Term};
        _Err ->
            {error, fail}
    end.

-spec pool_transaction(fun(), list(), Retries) -> {ok, term()} | {error, term()} when Retries :: non_neg_integer() | infinity.
pool_transaction(Fun, Args, Retries) when is_function(Fun, 1) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {transaction, Fun, Args, Retries}) end) of
        {atomic, Term} ->
            {ok, Term};
        {aborted, Term} ->
            {error, Term};
        _Err ->
            {error, fail}
    end.

-spec change_user(Conn, Username, Password) -> Result | {error, fail}
    when Conn :: mysql:connection(),
    Username :: iodata(),
    Password :: iodata(),
    Result :: ok.
change_user(Conn, Username, Password) ->
    case catch mysql:change_user(Conn, Username, Password) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec change_user(Conn, Username, Password, Options) -> Result | {error, fail}
    when Conn :: mysql:connection(),
    Username :: iodata(),
    Password :: iodata(),
    Options :: [Option],
    Result :: ok,
    Option :: {database, iodata()}
    | {queries, [iodata()]}
    | {prepare, [NamedStatement]},
    NamedStatement :: {StatementName :: atom(), Statement :: iodata()}.
change_user(Conn, Username, Password, Args) ->
    case catch mysql:change_user(Conn, Username, Password, Args) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec reset_connection(Conn) -> ok | {error, Reason}
    when Conn :: mysql:connection(),
    Reason :: server_reason().
reset_connection(Conn) ->
    case catch mysql:reset_connection(Conn) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec is_connected(Conn) -> boolean() | {error, fail}
    when Conn :: mysql:connection().
is_connected(Conn) ->
    case catch mysql:is_connected(Conn) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

%% @doc 从连接池中取出一个工作进程
-spec get_worker() -> {ok, pid()} | {error, fail}.
get_worker() ->
    case catch poolboy:checkout(?POOL_NAME) of
        Worker when is_pid(Worker) ->
            {ok, Worker};
        _Err ->
            {error, fail}
    end.

%% @doc 将工作进程放回连接池
-spec put_worker(pid()) -> ok.
put_worker(Worker) ->
    poolboy:checkin(?POOL_NAME, Worker).

%% @doc 获取连接进程
-spec get_conn(pid()) -> {ok, pid()} | {error, fail}.
get_conn(Worker) ->
    case catch ?DB_CALL(Worker, get_conn) of
        {ok, Conn} when is_pid(Conn) ->
            {ok, Conn};
        _Err ->
            {error, fail}
    end.
