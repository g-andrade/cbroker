-module(cbroker_nif).

-on_load(init/0).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    ask/4,
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

-type broker() :: broker_name() | broker_ref().
-export_type([broker/0]).

-type broker_name() :: (atom() | {global, term()} | {via, module(), term()}).
-export_type([broker_name/0]).

-opaque broker_ref() :: reference().
-export_type([broker_ref/0]).

-type broker_opt() ::
    (boolean_opt(depends_on_creator)).
-export_type([broker_opt/0]).

-type boolean_opt(Name) :: Name | {Name, boolean()}.
-export_type([boolean_opt/1]).

-type side() :: left | right.
-export_type([side/0]).

-type msg() :: {tag(), reply()}.
-export_type([msg/0]).

-type tag() :: reference().
-export_type([tag/0]).

-type reply() :: (match() | drop()).
-export_type([reply/0]).

-type match() :: match(term()).
-type match(CounterOffer) :: {match, match_ref(), CounterOffer, sojourn_time()}.
-export_type([match/0, match/1]).

-type match_ref() :: reference().
-export_type([match_ref/0]).

-type drop() :: {drop, drop_reason(), sojourn_time()}.
-export_type([drop/0]).

-type drop_reason() :: known_drop_reason() | Other :: term().
-export_type([drop_reason/0]).

-type known_drop_reason() ::
    (cancelled
    | match_unavailable
    | broker_overloaded
    | broker_closed).
-export_type([known_drop_reason/0]).

-type sojourn_time() :: non_neg_integer().
-export_type([sojourn_time/0]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

ask(BrokerRef, Side, Offer, AskType) ->
    OfferSizeArg = offer_size_arg(Offer),
    ask(BrokerRef, Side, Offer, OfferSizeArg, AskType).

-spec cancel(Tag) -> too_late | {cancelled, SojournTime} when
    Tag :: tag(),
    SojournTime :: sojourn_time().

cancel(_Tag) ->
    not_loaded(?LINE).

debug_info(_BrokerRef) ->
    not_loaded(?LINE).

-spec new(Opts) -> Broker when
    Opts :: [broker_opt()],
    Broker :: broker_ref().

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

ask(_BrokerRef, _Side, _Offer, _OfferSizeArg, _AskType) ->
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
