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
    {ok, {SizeArgs, WorkerArgs}} = application:get_env(db, pool),
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 10},

    PoolArgs = [{name, {local, db_pool}}, {worker_module, db_cli}] ++ SizeArgs,
    ChildSpecs = [poolboy:child_spec(db_pool, PoolArgs, WorkerArgs)],
    {ok, {SupFlags, ChildSpecs}}.
