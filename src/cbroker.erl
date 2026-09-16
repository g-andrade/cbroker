-module(cbroker).

-on_load(init/0).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    ask/2,
    ask/3,
    ask/4,
    %
    async_ask/2,
    async_ask/3,
    %
    await/1,
    await/2,
    %
    cancel/1,
    %
    child_spec/1,
    child_spec/2,
    %
    debug_info/1,
    %
    dynamic_ask/2,
    dynamic_ask/3,
    %
    nb_ask/2,
    nb_ask/3,
    %
    new/0,
    new/1,
    %
    resolve_name/1,
    %
    resumable_ask/2,
    resumable_ask/3,
    resumable_ask/4,
    %
    resumable_await/1,
    resumable_await/2
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(APPNAME, cbroker).
-define(LIBNAME, cbroker).

-define(DEFAULT_TIMEOUT, 5_000).

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

-spec ask(Broker, Side) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: timeout | drop_reason(),
    SojournTime :: sojourn_time().

ask(Broker, Side) ->
    ask(Broker, Side, self()).

%%

-spec ask(Broker, Side, Offer) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: timeout | drop_reason(),
    SojournTime :: sojourn_time().

ask(Broker, Side, Offer) ->
    ask(Broker, Side, Offer, ?DEFAULT_TIMEOUT).

%%

-spec ask(Broker, Side, Offer, Timeout) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    Timeout :: timeout(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: timeout | drop_reason(),
    SojournTime :: sojourn_time().

ask(Broker, Side, Offer, Timeout) ->
    case dynamic_ask(Broker, Side, Offer) of
        {await, Tag} ->
            await(Tag, Timeout);
        %
        Result ->
            Result
    end.

%%

-spec async_ask(Broker, Side) -> {await, Tag} when
    Broker :: broker(),
    Side :: side(),
    Tag :: tag().

async_ask(Broker, Side) ->
    async_ask(Broker, Side, self()).

%%

-spec async_ask(Broker, Side, Offer) -> {await, Tag} when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    Tag :: tag().

async_ask(Broker, Side, Offer) ->
    BrokerRef = resolve_broker(Broker),
    {await, _} = do_ask(BrokerRef, Side, Offer, async).

%%

-spec await(Tag) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Tag :: tag(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: timeout | drop_reason(),
    SojournTime :: sojourn_time().

await(Tag) ->
    await(Tag, ?DEFAULT_TIMEOUT).

%%

-spec await(Tag, Timeout) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Tag :: tag(),
    Timeout :: timeout(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: timeout | drop_reason(),
    SojournTime :: sojourn_time().

await(Tag, Timeout) ->
    receive
        {Ref, Reply} when Ref =:= Tag ->
            Reply
    after Timeout ->
        case cancel(Tag) of
            {cancelled, SojournTime} ->
                {drop, timeout, SojournTime};
            %
            {match, _, _, _} = Match ->
                Match
        end
    end.

%%

-spec cancel(Tag) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {cancelled, SojournTime}
when
    Tag :: tag(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time().

cancel(Tag) ->
    case nif_cancel(Tag) of
        too_late ->
            %
            case resumable_await(Tag, infinity) of
                {drop, _, SojournTime} ->
                    {cancelled, SojournTime};
                %
                {match, _, _, _} = Match ->
                    Match
            end;
        %
        {cancelled, _} = Cancelled ->
            Cancelled
    end.

%%

-spec child_spec(RegName) -> supervisor:child_spec() when
    RegName :: {local, atom()} | {global, term()} | {via, module(), term()}.

child_spec(Name) ->
    child_spec(Name, []).

%%

-spec child_spec(RegName, Opts) -> supervisor:child_spec() when
    RegName :: {local, atom()} | {global, term()} | {via, module(), term()},
    Opts :: [broker_opt()].

child_spec(Name, Opts) ->
    cbroker_persistent:child_spec(Name, Opts).

%%

-spec debug_info(Broker) -> term() when
    Broker :: broker().

debug_info(Broker) ->
    BrokerRef = resolve_broker(Broker),
    nif_debug_info(BrokerRef).

%%

-spec dynamic_ask(Broker, Side) ->
    {await, Tag}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    Tag :: tag(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

dynamic_ask(Broker, Side) ->
    dynamic_ask(Broker, Side, self()).

%%

-spec dynamic_ask(Broker, Side, Offer) ->
    {await, Tag}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    Tag :: tag(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

dynamic_ask(Broker, Side, Offer) ->
    BrokerRef = resolve_broker(Broker),
    do_ask(BrokerRef, Side, Offer, dynamic).

%%

-spec nb_ask(Broker, Side) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

nb_ask(Broker, Side) ->
    nb_ask(Broker, Side, self()).

-spec nb_ask(Broker, Side, Offer) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

nb_ask(Broker, Side, Offer) ->
    BrokerRef = resolve_broker(Broker),
    do_ask(BrokerRef, Side, Offer, non_blocking).

%%

-spec new() -> Broker when
    Broker :: broker_ref().

new() ->
    not_loaded(?LINE).

%%

-spec new(Opts) -> Broker when
    Opts :: [broker_opt()],
    Broker :: broker_ref().

new(_Opts) ->
    not_loaded(?LINE).

%%

-spec resolve_name(BrokerName) -> BrokerRef when
    BrokerName :: broker_name(),
    BrokerRef :: broker_ref().

resolve_name(BrokerName) ->
    cbroker_persistent:get(BrokerName).

%%

-spec resumable_ask(Broker, Side) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {timeout, Tag}
when
    Broker :: broker(),
    Side :: side(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    Tag :: tag().

resumable_ask(Broker, Side) ->
    resumable_ask(Broker, Side, self()).

%%

-spec resumable_ask(Broker, Side, Offer) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {timeout, Tag}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    Tag :: tag().

resumable_ask(Broker, Side, Offer) ->
    resumable_ask(Broker, Side, Offer, ?DEFAULT_TIMEOUT).

%%

-spec resumable_ask(Broker, Side, Offer, Timeout) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {timeout, Tag}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    Timeout :: timeout(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    Tag :: tag().

resumable_ask(Broker, Side, Offer, Timeout) ->
    case dynamic_ask(Broker, Side, Offer) of
        {await, Tag} ->
            case resumable_await(Tag, Timeout) of
                timeout ->
                    {timeout, Tag};
                %
                Result ->
                    Result
            end;
        %
        Result ->
            Result
    end.

%%

-spec resumable_await(Tag) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | timeout
when
    Tag :: tag(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

resumable_await(Tag) ->
    resumable_await(Tag, ?DEFAULT_TIMEOUT).

%%

-spec resumable_await(Tag, Timeout) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | timeout
when
    Tag :: tag(),
    Timeout :: timeout(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

resumable_await(Tag, Timeout) ->
    receive
        {T, Reply} when T =:= Tag ->
            Reply
    after Timeout ->
        timeout
    end.

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

%%

do_ask(BrokerRef, Side, Offer, AskType) ->
    OfferSizeArg = offer_size_arg(Offer),
    EnqueueTs = erlang:monotonic_time(nanosecond),
    RetryNr = 0,

    case nif_ask(BrokerRef, Side, Offer, OfferSizeArg, AskType, EnqueueTs, RetryNr) of
        {error, Reason} ->
            error(Reason);
        %
        Result ->
            Result
    end.

nif_ask(_BrokerRef, _Side, _Offer, _OffersizeArg, _AskType, _EnqueueTs, _RetryNr) ->
    not_loaded(?LINE).

-if(?OTP_RELEASE < 29).
offer_size_arg(Value) ->
    erts_debug:flat_size(Value).
-else.
offer_size_arg(_Value) ->
    compute_from_nif.
-endif.

%%

-spec nif_cancel(Tag) -> too_late | {cancelled, SojournTime} when
    Tag :: tag(),
    SojournTime :: sojourn_time().

nif_cancel(_Tag) ->
    not_loaded(?LINE).

nif_debug_info(_BrokerRef) ->
    not_loaded(?LINE).

%%

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

resolve_broker(BrokerRef) when is_reference(BrokerRef) ->
    BrokerRef;
resolve_broker(BrokerName) ->
    cbroker_persistent:get(BrokerName).
