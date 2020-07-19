%%%-------------------------------------------------------------------
%%% @author huangzaoyi
%%% @copyright (C) 2020, <COMPANY>
%%% @doc 数据库接口测试
%%%
%%% @end
%%% Created : 19. 7月 2020 9:51 下午
%%%-------------------------------------------------------------------
-module(db_test).
-author("huangzaoyi").

%% API
-export([test_transaction/0]).

test_transaction() ->
    Fun =
        fun(Conn) ->
            Ret1 = db:query(Conn, <<"SELECT * from user">>),
            Ret2 = db:query(Conn, <<"INSERT INTO user (user_name) values (\"abc\")">>),
            Ret3 = db:query(Conn, <<"SELECT * from user">>),
            {Ret1, Ret2, Ret3}
        end,
    Ret = db:pool_transaction(Fun),
    io:format("返回值:~w~n", [Ret]).

