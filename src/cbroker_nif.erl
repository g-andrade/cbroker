-module(cbroker_nif).

-export([
    new/0,
    new/1,
    ask/3,
    ask/4,
    cancel/1,
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

-type ask_type() :: regular | nb | fully_async.
-export_type([ask_type/0]).

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
    | closed).
-export_type([async_reply_content/0]).

%%

new() ->
    not_loaded(?LINE).

new(_Opts) ->
    not_loaded(?LINE).

ask(Broker, Side, Value) ->
    SizeArg = size_args(Value),
    do_ask(Broker, Side, Value, SizeArg).

-spec ask(Broker, Side, Value, AskType) ->
    {await, Tag}
    | match()
    | retry
    | cancelled
    | closed
when
    Broker :: broker(),
    Side :: side(),
    Value :: term(),
    AskType :: ask_type(),
    Tag :: tag().
ask(Broker, Side, Value, AskType) ->
    SizeArg = erts_debug:size(Value),
    do_ask(Broker, Side, Value, SizeArg, AskType).

-spec cancel(Tag) -> cancelled | too_late when
    Tag :: tag().
cancel(_Tag) ->
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

-if(?OTP_RELEASE < 29).
size_args(Value) ->
    erts_debug:flat_size(Value).
-else.
size_args(_Value) ->
    compute_from_nif.
-endif.

do_ask(_Broker, _Side, _Value, _SizeArg) ->
    not_loaded(?LINE).

do_ask(_Broker, _Side, _Value, _SizeArg, _AskType) ->
    not_loaded(?LINE).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
