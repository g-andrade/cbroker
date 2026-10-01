%% @moduledoc false
-module(cbroker_nif).

-on_load(init/0).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-ignore_xref([
    alloc_perfcounters/0
]).

-export([
    alloc_perfcounters/0,
    ask/5,
    cancel/1,
    debug_info/1,
    new/1
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(APPNAME, cbroker).
-define(LIBNAME, cbroker).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% Static Check Tweaks
%% ------------------------------------------------------------------

-if(?OTP_RELEASE >= 29).
-hank([{unnecessary_function_arguments, [{offer_size_arg, 1, 1}]}]).
-endif.

-hank([
    {unnecessary_function_arguments, [
        ask,
        cancel,
        debug_info,
        new
    ]}
]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

% Live allocations across every broker, for the tests to assert nothing leaked
-spec alloc_perfcounters() -> list() | unavailable.
alloc_perfcounters() ->
    not_loaded(?LINE).

%%

-spec ask(BrokerRef, Lane, Offer, ReplyRef, AskType) ->
    {await, Ticket}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {error, closed}
when
    BrokerRef :: reference(),
    Lane :: left | right,
    Offer :: term(),
    ReplyRef :: ticket | reference(),
    AskType :: dynamic | async | non_blocking,
    Ticket :: reference(),
    MatchRef :: reference(),
    CounterOffer :: term(),
    DropReason :: cbroker:drop_reason(),
    SojournTime :: non_neg_integer().

ask(BrokerRef, Lane, Offer, ReplyRef, AskType) ->
    OfferSizeArg = offer_size_arg(Offer),
    ask(BrokerRef, Lane, Offer, OfferSizeArg, ReplyRef, AskType).

%%

-spec cancel(Ticket) -> {cancelled, SojournTime} | too_late when
    Ticket :: reference(),
    SojournTime :: non_neg_integer().

cancel(_Ticket) ->
    not_loaded(?LINE).

%%

-spec debug_info(BrokerRef) -> DebugInfo when
    BrokerRef :: reference(),
    DebugInfo :: term().

debug_info(_BrokerRef) ->
    not_loaded(?LINE).

%%

-spec new([Opt]) -> BrokerRef when
    Opt :: term(),
    BrokerRef :: reference().

new(_Opts) ->
    not_loaded(?LINE).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

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

ask(_BrokerRef, _Lane, _Offer, _OfferSizeArg, _ReplyRef, _AskType) ->
    not_loaded(?LINE).

-if(?OTP_RELEASE < 29).
offer_size_arg(Value) ->
    erts_debug:flat_size(Value).
-else.
offer_size_arg(_Value) ->
    compute_from_nif.
-endif.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
