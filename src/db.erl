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
    query/1, query/2, query/3, query/4,
    execute/2, execute/3, execute/4
]).

-define(DB_CALL(Cli_Pid, Call), gen_server:call(Cli_Pid, Call, 5000)).
-define(POOL_NAME, db_pool).

-spec query(Query) -> Result
    when Query :: iodata(),
    Result :: query_result().
query(Query) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec query(Query, Args | FilterMap | Timeout) -> Result
    when Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result().
query(Query, Args) when is_list(Args) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Args}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
query(Query, FilterMap) when is_function(FilterMap, 1) orelse is_function(FilterMap, 2) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, FilterMap}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
query(Query, Timeout) when is_integer(Timeout) orelse Timeout == infinity ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec query(Query, Args, Timeout) -> Result
    when Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    Result :: query_result();
    (Conn, Query, FilterMap, Timeout) -> Result
    when Query :: iodata(),
    Timeout :: timeout(),
    FilterMap :: query_filtermap_fun(),
    Result :: query_result();
    (Conn, Query, Args, FilterMap) -> Result
    when Query :: iodata(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result().
query(Query, Args, Timeout) when is_list(Args) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Args, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
query(Query, FilterMap, Timeout) when (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, FilterMap, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
query(Query, Args, FilterMap) when is_list(Args) andalso (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Args, FilterMap}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec query(Query, Args, FilterMap, Timeout) -> Result
    when Query :: iodata(),
    Timeout :: timeout(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Result :: query_result().
query(Query, Args, FilterMap, Timeout) when is_list(Args) andalso (is_function(FilterMap, 1) orelse is_function(FilterMap, 2)) andalso (is_integer(Timeout) orelse Timeout == infinity) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {query, Query, Args, FilterMap, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec execute(Ref, Args) -> Result | {error, term()}
    when Ref :: atom() | integer(),
    Args :: [term()],
    Result :: query_result().
execute(Ref, Args) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {execute, Ref, Args}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec execute(Ref, Args, FilterMap | Timeout) ->
    Result | {error, term()}
    when Ref :: atom() | integer(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Timeout :: timeout(),
    Result :: query_result().
execute(Ref, Args, Timeout) when is_integer(Timeout) orelse Timeout == infinity ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {execute, Ref, Args, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end;
execute(Ref, Args, FilterMap) when is_function(FilterMap, 1) orelse is_function(FilterMap, 2) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {execute, Ref, Args, FilterMap}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.

-spec execute(Ref, Args, FilterMap, Timeout) ->
    Result | {error, term()}
    when Ref :: atom() | integer(),
    Args :: [term()],
    FilterMap :: query_filtermap_fun(),
    Timeout :: timeout(),
    Result :: query_result().
execute(Ref, Args, FilterMap, Timeout) ->
    case catch poolboy:transaction(?POOL_NAME, fun(Cli) -> ?DB_CALL(Cli, {execute, Ref, Args, FilterMap, Timeout}) end) of
        {'EXIT', _Err} ->
            {error, fail};
        Ret ->
            Ret
    end.


