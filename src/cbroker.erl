-module(cbroker).

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
    async_ask/4,
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
    dynamic_ask/4,
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
    resumable_ask/4
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

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

-type tag() :: reply_ref() | ticket().
-export_type([tag/0]).

-type reply_ref() :: reference().
-export_type([reply_ref/0]).

-opaque ticket() :: reference().
-export_type([ticket/0]).

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
    BrokerRef = resolve_broker(Broker),
    ReplyRef = make_ref(),

    case do_ask(BrokerRef, Side, Offer, ReplyRef, dynamic) of
        {await, Ticket} ->
            ask_await(ReplyRef, Ticket, Timeout);
        %
        Result ->
            Result
    end.
%%

-spec async_ask(Broker, Side) -> {await, Ticket} when
    Broker :: broker(),
    Side :: side(),
    Ticket :: ticket().

async_ask(Broker, Side) ->
    async_ask(Broker, Side, self()).

%%

-spec async_ask(Broker, Side, Offer) -> {await, Ticket} when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    Ticket :: ticket().

async_ask(Broker, Side, Offer) ->
    async_ask(Broker, Side, Offer, ticket).

%%

-spec async_ask(Broker, Side, Offer, ReplyRef) -> {await, Ticket} when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    ReplyRef :: ticket | reference(),
    Ticket :: ticket().

async_ask(Broker, Side, Offer, ReplyRef) ->
    BrokerRef = resolve_broker(Broker),
    {await, _} = do_ask(BrokerRef, Side, Offer, ReplyRef, async).

%%

-spec cancel(Ticket) -> {cancelled, SojournTime} | too_late when
    Ticket :: ticket(),
    SojournTime :: sojourn_time().

cancel(Ticket) ->
    case cbroker_nif:cancel(Ticket) of
        {cancelled, _} = Cancelled ->
            Cancelled;
        %
        too_late ->
            too_late
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
    cbroker_nif:debug_info(BrokerRef).

%%

-spec dynamic_ask(Broker, Side) ->
    {await, Ticket}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    Ticket :: ticket(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

dynamic_ask(Broker, Side) ->
    dynamic_ask(Broker, Side, self()).

%%

-spec dynamic_ask(Broker, Side, Offer) ->
    {await, Ticket}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    Ticket :: ticket(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

dynamic_ask(Broker, Side, Offer) ->
    dynamic_ask(Broker, Side, Offer, ticket).

%%

-spec dynamic_ask(Broker, Side, Offer, ReplyRef) ->
    {await, Ticket}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    ReplyRef :: ticket | reference(),
    Ticket :: ticket(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

dynamic_ask(Broker, Side, Offer, ReplyRef) ->
    BrokerRef = resolve_broker(Broker),
    do_ask(BrokerRef, Side, Offer, ReplyRef, dynamic).

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
    do_ask(BrokerRef, Side, Offer, ticket, non_blocking).

%%

new() ->
    new([]).

%%

-spec new(Opts) -> Broker when
    Opts :: [broker_opt()],
    Broker :: broker_ref().

new(Opts) ->
    cbroker_nif:new(Opts).

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
    | {timeout, ReplyRef, Ticket}
when
    Broker :: broker(),
    Side :: side(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    ReplyRef :: reference(),
    Ticket :: ticket().

resumable_ask(Broker, Side) ->
    resumable_ask(Broker, Side, self()).

%%

-spec resumable_ask(Broker, Side, Offer) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {timeout, ReplyRef, Ticket}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    ReplyRef :: reference(),
    Ticket :: ticket().

resumable_ask(Broker, Side, Offer) ->
    resumable_ask(Broker, Side, Offer, ?DEFAULT_TIMEOUT).

%%

-spec resumable_ask(Broker, Side, Offer, Timeout) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {timeout, ReplyRef, Ticket}
when
    Broker :: broker(),
    Side :: side(),
    Offer :: term(),
    Timeout :: timeout(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    ReplyRef :: reference(),
    Ticket :: ticket().

resumable_ask(Broker, Side, Offer, Timeout) ->
    BrokerRef = resolve_broker(Broker),
    ReplyRef = make_ref(),

    case do_ask(BrokerRef, Side, Offer, ReplyRef, dynamic) of
        {await, Ticket} ->
            resumable_ask_await(ReplyRef, Ticket, Timeout);
        %
        Result ->
            Result
    end.

%%

% -spec resumable_await(Ticket) ->
%     {match, MatchRef, CounterOffer, SojournTime}
%     | {drop, DropReason, SojournTime}
%     | timeout
% when
%     Ticket :: ticket(),
%     MatchRef :: match_ref(),
%     CounterOffer :: term(),
%     DropReason :: drop_reason(),
%     SojournTime :: sojourn_time().
%
% resumable_await(Ticket) ->
%     resumable_await(Ticket, ?DEFAULT_TIMEOUT).
%
% %%
%
% -spec resumable_await(Ticket, Timeout) ->
%     {match, MatchRef, CounterOffer, SojournTime}
%     | {drop, DropReason, SojournTime}
%     | timeout
% when
%     Ticket :: ticket(),
%     Timeout :: timeout(),
%     MatchRef :: match_ref(),
%     CounterOffer :: term(),
%     DropReason :: drop_reason(),
%     SojournTime :: sojourn_time().
%
% resumable_await(Ticket, Timeout) ->
%     receive
%         {T, Reply} when T =:= Ticket ->
%             Reply
%     after Timeout ->
%         timeout
%     end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

ask_await(ReplyRef, Ticket, Timeout) ->
    receive
        {ReplyRef, Reply} ->
            Reply
    after Timeout ->
        ask_timeout(ReplyRef, Ticket)
    end.

ask_timeout(ReplyRef, Ticket) ->
    case cbroker_nif:cancel(Ticket) of
        {cancelled, SojournTime} ->
            {drop, timeout, SojournTime};
        %
        too_late ->
            receive
                {ReplyRef, Reply} ->
                    Reply
            end
    end.

%%

do_ask(BrokerRef, Side, Offer, ReplyRef, AskType) ->
    case cbroker_nif:ask(BrokerRef, Side, Offer, ReplyRef, AskType) of
        {error, Reason} ->
            error(Reason);
        %
        Result ->
            Result
    end.

%%

resolve_broker(BrokerRef) when is_reference(BrokerRef) ->
    BrokerRef;
resolve_broker(BrokerName) ->
    cbroker_persistent:get(BrokerName).

%%

resumable_ask_await(ReplyRef, Ticket, Timeout) ->
    receive
        {ReplyRef, Reply} ->
            Reply
    after Timeout ->
        {timeout, ReplyRef, Ticket}
    end.
