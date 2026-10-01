%% Copyright (c) 2026 Guilherme Andrade
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the "Software"),
%% to deal in the Software without restriction, including without limitation
%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%% and/or sell copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
%% DEALINGS IN THE SOFTWARE.

-module(cbroker).

-ifdef(E48).
-moduledoc """
Brokers that match processes asking on opposite lanes.

Matches are made concurrently (see [Internals](INTERNALS.md)).

A broker has two lanes, `left` and `right`. An ask brings an offer to one lane
and is matched with one of the oldest asks waiting on the other, each side
getting the other's offer.

The ask functions differ in what they do when there is no match yet:
- `ask/4` waits,
- `nb_ask/3` gives up,
- `dynamic_ask/4`, `async_ask/4` and `resumable_ask/4` leave the ask enqueued
  and have the reply sent as a message.
""".
-endif.

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

-ignore_xref([
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

%%

-ifdef(E48).
-doc "The name a broker was given through `child_spec/2`.".
-endif.

-type broker_name() :: (atom() | {global, term()} | {via, module(), term()}).
-export_type([broker_name/0]).

%%

-ifdef(E48).
-doc "A broker, as returned by `new/1`.".
-endif.

-opaque broker_ref() :: reference().
-export_type([broker_ref/0]).

%%

-ifdef(E48).
-doc """
- `depends_on_creator`: close the broker when the process that created it dies.
  Defaults to `false`.
- `max_queue_len`: how many more asks one lane may have waiting than the other.
  Defaults to `unlimited`.
- `min_left_balance`, `max_right_balance`: the same limit, set for each lane
  apart. The first is a negative number.
- `cells_per_batch`: how many cells each batch has. Defaults to 32 per
  scheduler.
- `ask_credits`: how many cells an ask may try before yielding. Defaults to 400.
- `ask_max_tries`: how many times an ask may run out of credits before it is
  dropped with `too_many_tries`. Defaults to 10.
- `batch_pool`, `request_pool`, `ticket_pool`: how many spare allocations of
  each kind are kept for reuse.
""".
-endif.

-type broker_opt() ::
    (depends_on_creator
    | {depends_on_creator, boolean()}
    | {cells_per_batch, pos_integer()}
    | {ask_credits, pos_integer()}
    | {ask_max_tries, pos_integer()}
    | {max_queue_len, pos_integer() | unlimited}
    | {min_left_balance, neg_integer() | unlimited}
    | {max_right_balance, pos_integer() | unlimited}
    | {batch_pool, [broker_pool_opt()]}
    | {request_pool, [broker_pool_opt()]}
    | {ticket_pool, [broker_pool_opt()]}).
-export_type([broker_opt/0]).

%%

-ifdef(E48).
-doc "A pool keeps up to `size` spares, `initial_count` of them allocated upfront.".
-endif.

-type broker_pool_opt() ::
    ({size, non_neg_integer()}
    | {initial_count, non_neg_integer()}).
-export_type([broker_pool_opt/0]).

%%

-ifdef(E48).
-doc "An ask on one lane only matches asks on the other.".
-endif.

-type lane() :: left | right.
-export_type([lane/0]).

%%

-ifdef(E48).
-doc "A reply to an enqueued ask, as it arrives in the asker's mailbox.".
-endif.

-type msg() :: {tag(), reply()}.
-export_type([msg/0]).

%%

-ifdef(E48).
-doc "What a reply message is tagged with: the `ReplyRef` if one was given, or else the ticket.".
-endif.

-type tag() :: reply_ref() | ticket().
-export_type([tag/0]).

%%

-type reply_ref() :: reference().
-export_type([reply_ref/0]).

%%

-ifdef(E48).
-doc "Identifies an enqueued ask, to `cancel/1` it.".
-endif.

-opaque ticket() :: reference().
-export_type([ticket/0]).

%%

-type reply() :: (match() | drop()).
-export_type([reply/0]).

%%

-type match() :: match(term()).
-type match(CounterOffer) :: {match, match_ref(), CounterOffer, sojourn_time()}.
-export_type([match/0, match/1]).

%%

-ifdef(E48).
-doc "Unique to a match, and the same for both of its sides.".
-endif.
-type match_ref() :: reference().
-export_type([match_ref/0]).

%%

-type drop() :: {drop, drop_reason(), sojourn_time()}.
-export_type([drop/0]).

-type drop_reason() :: known_drop_reason() | Other :: term().
-export_type([drop_reason/0]).

%%

-ifdef(E48).
-doc """
- `cancelled`: another process cancelled the ask.
- `closed`: the broker closed while the ask was enqueued.
- `full_lane`: the lane already has as many asks waiting as its limit allows.
- `too_many_tries`: the ask tried too many cells without managing to match or
  enqueue.
""".
-endif.
-type known_drop_reason() ::
    (cancelled
    | closed
    | full_lane
    | too_many_tries).
-export_type([known_drop_reason/0]).

%%

-ifdef(E48).
-doc "Nanoseconds between asking and the reply.".
-endif.

-type sojourn_time() :: non_neg_integer().
-export_type([sojourn_time/0]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-ifdef(E48).
-doc #{equiv => ask(Broker, Lane, self())}.
-endif.

-spec ask(Broker, Lane) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Lane :: lane(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: timeout | drop_reason(),
    SojournTime :: sojourn_time().

ask(Broker, Lane) ->
    ask(Broker, Lane, self()).

%%

-ifdef(E48).
-doc #{equiv => ask(Broker, Lane, Offer, 5000)}.
-endif.

-spec ask(Broker, Lane, Offer) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: timeout | drop_reason(),
    SojournTime :: sojourn_time().

ask(Broker, Lane, Offer) ->
    ask(Broker, Lane, Offer, ?DEFAULT_TIMEOUT).

%%

-ifdef(E48).
-doc """
Asks on `Lane`, waiting up to `Timeout` milliseconds for a match.

If there is none by then, the ask is cancelled and `{drop, timeout, _}` is
returned.
""".
-endif.

-spec ask(Broker, Lane, Offer, Timeout) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    Timeout :: timeout(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: timeout | drop_reason(),
    SojournTime :: sojourn_time().

ask(Broker, Lane, Offer, Timeout) ->
    BrokerRef = resolve_broker(Broker),
    ReplyRef = make_ref(),

    case do_ask(BrokerRef, Lane, Offer, ReplyRef, dynamic) of
        {await, Ticket} ->
            ask_await(ReplyRef, Ticket, Timeout);
        %
        Result ->
            Result
    end.
%%

-ifdef(E48).
-doc #{equiv => async_ask(Broker, Lane, self())}.
-endif.

-spec async_ask(Broker, Lane) -> {await, Ticket} when
    Broker :: broker(),
    Lane :: lane(),
    Ticket :: ticket().

async_ask(Broker, Lane) ->
    async_ask(Broker, Lane, self()).

%%

-ifdef(E48).
-doc #{equiv => async_ask(Broker, Lane, Offer, ticket)}.
-endif.

-spec async_ask(Broker, Lane, Offer) -> {await, Ticket} when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    Ticket :: ticket().

async_ask(Broker, Lane, Offer) ->
    async_ask(Broker, Lane, Offer, ticket).

%%

-ifdef(E48).
-doc """
Asks on `Lane` and returns at once, whatever the outcome.

The reply, be it a match or a drop, arrives as a `t:msg/0`. It is tagged with
`ReplyRef` if that is a reference, or with the returned ticket if it is the atom
`ticket`.
""".
-endif.

-spec async_ask(Broker, Lane, Offer, ReplyRef) -> {await, Ticket} when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    ReplyRef :: ticket | reference(),
    Ticket :: ticket().

async_ask(Broker, Lane, Offer, ReplyRef) ->
    BrokerRef = resolve_broker(Broker),
    {await, _} = do_ask(BrokerRef, Lane, Offer, ReplyRef, async).

%%

-ifdef(E48).
-doc """
Withdraws an enqueued ask.

Returns `too_late` if the ask was already matched or dropped, in which case its
reply has been sent.

When called by a process other than the asker, the asker gets a
`{drop, cancelled, _}` reply.
""".
-endif.

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

-ifdef(E48).
-doc #{equiv => child_spec(RegName, [])}.
-endif.

-spec child_spec(RegName) -> supervisor:child_spec() when
    RegName :: {local, atom()} | {global, term()} | {via, module(), term()}.

child_spec(Name) ->
    child_spec(Name, []).

%%

-ifdef(E48).
-doc """
Returns the child spec of a named broker, to place under a supervisor.

Once started, the broker can be asked by its name. It closes when the child
stops.
""".
-endif.

-spec child_spec(RegName, Opts) -> supervisor:child_spec() when
    RegName :: {local, atom()} | {global, term()} | {via, module(), term()},
    Opts :: [broker_opt()].

child_spec(Name, Opts) ->
    cbroker_persistent:child_spec(Name, Opts).

%%

-ifdef(E48).
-doc "Returns a broker's internal state, for debugging. Its shape may change at any time.".
-endif.

-spec debug_info(Broker) -> term() when
    Broker :: broker().

debug_info(Broker) ->
    BrokerRef = resolve_broker(Broker),
    cbroker_nif:debug_info(BrokerRef).

%%

-ifdef(E48).
-doc #{equiv => dynamic_ask(Broker, Lane, self())}.
-endif.

-spec dynamic_ask(Broker, Lane) ->
    {await, Ticket}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Lane :: lane(),
    Ticket :: ticket(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

dynamic_ask(Broker, Lane) ->
    dynamic_ask(Broker, Lane, self()).

%%

-ifdef(E48).
-doc #{equiv => dynamic_ask(Broker, Lane, Offer, ticket)}.
-endif.

-spec dynamic_ask(Broker, Lane, Offer) ->
    {await, Ticket}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    Ticket :: ticket(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

dynamic_ask(Broker, Lane, Offer) ->
    dynamic_ask(Broker, Lane, Offer, ticket).

%%

-ifdef(E48).
-doc """
Asks on `Lane`, returning the match if there is one already.

Otherwise the ask is enqueued and `{await, Ticket}` returned, with the reply
arriving later as a `t:msg/0`, tagged as in `async_ask/4`.
""".
-endif.

-spec dynamic_ask(Broker, Lane, Offer, ReplyRef) ->
    {await, Ticket}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    ReplyRef :: ticket | reference(),
    Ticket :: ticket(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: drop_reason(),
    SojournTime :: sojourn_time().

dynamic_ask(Broker, Lane, Offer, ReplyRef) ->
    BrokerRef = resolve_broker(Broker),
    do_ask(BrokerRef, Lane, Offer, ReplyRef, dynamic).

%%

-ifdef(E48).
-doc #{equiv => nb_ask(Broker, Lane, self())}.
-endif.

-spec nb_ask(Broker, Lane) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Lane :: lane(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: match_not_found | drop_reason(),
    SojournTime :: sojourn_time().

nb_ask(Broker, Lane) ->
    nb_ask(Broker, Lane, self()).

%%

-ifdef(E48).
-doc """
Asks on `Lane` without ever waiting.

Returns the match if there is one already, or else `{drop, match_not_found, _}`.
""".
-endif.

-spec nb_ask(Broker, Lane, Offer) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    DropReason :: match_not_found | drop_reason(),
    SojournTime :: sojourn_time().

nb_ask(Broker, Lane, Offer) ->
    BrokerRef = resolve_broker(Broker),
    do_ask(BrokerRef, Lane, Offer, ticket, non_blocking).

%%

-ifdef(E48).
-doc #{equiv => new([])}.
-endif.

-spec new() -> Broker when
    Broker :: broker_ref().

new() ->
    new([]).

%%

-ifdef(E48).
-doc """
Creates a broker.

It lives for as long as it is referenced, a waiting ask counting as a reference.
With the `depends_on_creator` option, it closes when the calling process dies.
""".
-endif.

-spec new(Opts) -> Broker when
    Opts :: [broker_opt()],
    Broker :: broker_ref().

new(Opts) ->
    cbroker_nif:new(Opts).

%%

-ifdef(E48).
-doc """
Returns the reference of a named broker.

Asking through the reference saves the lookup that asking by name does.
""".
-endif.

-spec resolve_name(BrokerName) -> BrokerRef when
    BrokerName :: broker_name(),
    BrokerRef :: broker_ref().

resolve_name(BrokerName) ->
    cbroker_persistent:get(BrokerName).

%%

-ifdef(E48).
-doc #{equiv => resumable_ask(Broker, Lane, self())}.
-endif.

-spec resumable_ask(Broker, Lane) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {timeout, ReplyRef, Ticket}
when
    Broker :: broker(),
    Lane :: lane(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    ReplyRef :: reference(),
    Ticket :: ticket().

resumable_ask(Broker, Lane) ->
    resumable_ask(Broker, Lane, self()).

%%

-ifdef(E48).
-doc #{equiv => resumable_ask(Broker, Lane, Offer, 5000)}.
-endif.

-spec resumable_ask(Broker, Lane, Offer) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {timeout, ReplyRef, Ticket}
when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    ReplyRef :: reference(),
    Ticket :: ticket().

resumable_ask(Broker, Lane, Offer) ->
    resumable_ask(Broker, Lane, Offer, ?DEFAULT_TIMEOUT).

%%

-ifdef(E48).
-doc """
Asks on `Lane`, waiting up to `Timeout` milliseconds for a match, like `ask/4`.

If there is none by then, the ask stays enqueued and
`{timeout, ReplyRef, Ticket}` is returned. The reply will arrive as
`{ReplyRef, Reply}`, unless the ask is cancelled with `cancel/1` first.
""".
-endif.

-spec resumable_ask(Broker, Lane, Offer, Timeout) ->
    {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {timeout, ReplyRef, Ticket}
when
    Broker :: broker(),
    Lane :: lane(),
    Offer :: term(),
    Timeout :: timeout(),
    MatchRef :: match_ref(),
    CounterOffer :: term(),
    SojournTime :: sojourn_time(),
    DropReason :: drop_reason(),
    ReplyRef :: reference(),
    Ticket :: ticket().

resumable_ask(Broker, Lane, Offer, Timeout) ->
    BrokerRef = resolve_broker(Broker),
    ReplyRef = make_ref(),

    case do_ask(BrokerRef, Lane, Offer, ReplyRef, dynamic) of
        {await, Ticket} ->
            resumable_ask_await(ReplyRef, Ticket, Timeout);
        %
        Result ->
            Result
    end.

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
            % Matched or dropped in the meantime, so the reply is on its way
            receive
                {ReplyRef, Reply} ->
                    Reply
            end
    end.

%%

do_ask(BrokerRef, Lane, Offer, ReplyRef, AskType) ->
    case cbroker_nif:ask(BrokerRef, Lane, Offer, ReplyRef, AskType) of
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
