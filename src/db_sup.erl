%%%-------------------------------------------------------------------
%% @doc db top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(db_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok, DBPools} = application:get_env(db, pools),
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 10},
    Fun =
        fun({SizeArgs, WorkerArgs}) ->
            PoolArgs = [{name, {local, db_pool}}, {worker_module, db_cli}] ++ SizeArgs,
            poolboy:child_spec(db_pool, PoolArgs, WorkerArgs)
        end,
    ChildSpecs = lists:map(Fun, DBPools),
    {ok, {SupFlags, ChildSpecs}}.
