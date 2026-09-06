-module(cbroker_nif).

-export([
    new/0,
    ask/4,
    ask/5,
    cancel/2,
    to_list/1
]).

-on_load(init/0).

-define(APPNAME, cbroker).
-define(LIBNAME, cbroker).

%%

-opaque broker() :: reference().
-export_type([broker/0]).

-type side() :: left | right.
-export_type([side/0]).

-opaque tag() :: {side(), batch_id(), offset()}.
-export_type([tag/0]).

-type batch_id() :: pos_integer().
-type offset() :: pos_integer().

-type match() :: {match, match_ref(), ExchangeValue :: term()}.
-export_type([match/0]).

-type match_with_stats() ::
    {match, match_ref(), ExchangeValue :: term(), SojournTime :: non_neg_integer()}.
-export_type([match_with_stats/0]).

-type match_ref() :: reference().
-export_type([match_ref/0]).

-type async_reply() :: {tag(), async_reply_content()}.
-export_type([async_reply/0]).

-type async_reply_content() ::
    (match()
    | match_with_stats()
    | cancelled).
-export_type([async_reply_content/0]).

%%

new() ->
    not_loaded(?LINE).

-spec ask(Broker, Side, Value, WithStats) ->
    {await, Tag}
    | match()
    | retry
when
    Broker :: broker(),
    Side :: side(),
    Value :: term(),
    WithStats :: boolean(),
    Tag :: tag().
ask(_Broker, _Side, _Value, _WithStats) ->
    not_loaded(?LINE).

-spec ask(Broker, Side, Value, WithStats, true) ->
    {await, Tag}
    | retry
when
    Broker :: broker(),
    Side :: side(),
    Value :: term(),
    WithStats :: boolean(),
    Tag :: tag().
ask(_Broker, _Side, _Value, _WithStats, _IsFullyAsync) ->
    not_loaded(?LINE).

-spec cancel(Broker, Tag) -> cancelled | too_late when
    Broker :: broker(),
    Tag :: tag().
cancel(_Broker, _Tag) ->
    not_loaded(?LINE).

to_list(_Broker) ->
    not_loaded(?LINE).

%%

init() ->
    SoName =
        case code:priv_dir(?APPNAME) of
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
