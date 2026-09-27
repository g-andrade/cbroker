%% @copyright 2026 Guilherme Andrade
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy  of this software and associated documentation files (the "Software"),
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

%% Stateless properties over a single broker. Sequences of operations and
%% concurrency are covered elsewhere: a `proper_statem` model and a stress
%% suite, respectively.

-module(prop_cbroker).

-include_lib("proper/include/proper.hrl").

%% ------------------------------------------------------------------
%% Property Exports
%% ------------------------------------------------------------------

-export([
    prop_offers_survive_the_exchange/0,
    prop_sojourn_times_are_plausible/0,
    prop_nb_ask_on_an_idle_broker_drops/0,
    prop_match_refs_are_unique/0
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

% Matches are local and instantaneous, so a sojourn time anywhere near this
% means the arithmetic is wrong rather than the machine being slow
-define(MAX_PLAUSIBLE_SOJOURN_NS, 10_000_000_000).

-define(REPLY_TIMEOUT_MS, 5_000).

%% ------------------------------------------------------------------
%% Properties
%% ------------------------------------------------------------------

prop_offers_survive_the_exchange() ->
    ?FORALL(
        {LeftOffer, RightOffer},
        {offer(), offer()},
        begin
            {LeftReply, RightReply} = exchange(LeftOffer, RightOffer),
            {match, _, GotByLeft, _} = LeftReply,
            {match, _, GotByRight, _} = RightReply,

            GotByLeft =:= RightOffer andalso GotByRight =:= LeftOffer
        end
    ).

prop_sojourn_times_are_plausible() ->
    ?FORALL(
        {LeftOffer, RightOffer},
        {offer(), offer()},
        begin
            {LeftReply, RightReply} = exchange(LeftOffer, RightOffer),
            {match, _, _, LeftSojourn} = LeftReply,
            {match, _, _, RightSojourn} = RightReply,

            is_plausible_sojourn(LeftSojourn) andalso is_plausible_sojourn(RightSojourn)
        end
    ).

prop_nb_ask_on_an_idle_broker_drops() ->
    ?FORALL(
        {Lane, Offer},
        {lane(), offer()},
        begin
            {drop, Reason, Sojourn} = cbroker:nb_ask(broker(), Lane, Offer),

            Reason =:= match_unavailable andalso is_plausible_sojourn(Sojourn)
        end
    ).

prop_match_refs_are_unique() ->
    ?FORALL(
        Offers,
        non_empty(list(offer())),
        begin
            MatchRefs = [
                begin
                    {LeftReply, _} = exchange(Offer, Offer),
                    {match, MatchRef, _, _} = LeftReply,
                    MatchRef
                end
             || Offer <- Offers
            ],

            length(lists:usort(MatchRefs)) =:= length(MatchRefs)
        end
    ).

%% ------------------------------------------------------------------
%% Generators
%% ------------------------------------------------------------------

% `any()` covers immediates, floats, atoms, binaries, lists and tuples; maps,
% bitstrings and larger terms are added because the offer size accounting
% treats them differently
offer() ->
    frequency([
        {6, any()},
        {1, map(any(), any())},
        {1, bitstring()},
        {1, ?SIZED(Size, resize(Size * 20, list(any())))},
        {1, ?LET(Bytes, range(0, 2_000), binary(Bytes))}
    ]).

lane() ->
    oneof([left, right]).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% One offer parked on the left, then matched from the right; both replies are
% collected, leaving the broker idle again
exchange(LeftOffer, RightOffer) ->
    Broker = broker(),
    {await, Ticket} = cbroker:async_ask(Broker, left, LeftOffer),
    RightReply = cbroker:nb_ask(Broker, right, RightOffer),
    {await_reply(Ticket), RightReply}.

await_reply(Ticket) ->
    receive
        {Ticket, Reply} ->
            Reply
    after ?REPLY_TIMEOUT_MS ->
        error({no_reply_for, Ticket})
    end.

is_plausible_sojourn(SojournTime) ->
    is_integer(SojournTime) andalso
        SojournTime >= 0 andalso
        SojournTime < ?MAX_PLAUSIBLE_SOJOURN_NS.

% One broker for the whole property run: creating one per test would dominate
% the run time, and every test leaves it idle
broker() ->
    case erlang:get(broker) of
        undefined ->
            Broker = cbroker:new(),
            erlang:put(broker, Broker),
            Broker;
        %
        Broker ->
            Broker
    end.
