-module(cbroker_nif).

-export([
    new/0,
    ask/3,
    ask/4,
    cancel/2,
    to_list/1
]).

-on_load(init/0).

-define(APPNAME, cbroker).
-define(LIBNAME, cbroker).

%%

new() ->
    not_loaded(?LINE).

ask(_Broker, _Side, _Value) ->
    not_loaded(?LINE).

ask(_Broker, _Side, _Value, _IsFullyAsync) ->
    not_loaded(?LINE).

cancel(_Broker, _Ticket) ->
    not_loaded(?LINE).

to_list(_Broker) ->
    not_loaded(?LINE).

%%

init() ->
    SoName = case code:priv_dir(?APPNAME) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, ?LIBNAME]);
                _ ->
                    filename:join([priv, ?LIBNAME])
            end;
        Dir ->
            filename:join(Dir, ?LIBNAME)
    end,
    erlang:load_nif(SoName, 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
