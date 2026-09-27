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

%% A `proper_statem` model of one broker, driven from a single process.
%%
%% The model keeps at most one outstanding request per side, which makes every
%% outcome predictable: an ask matches exactly when the opposite side has a
%% request parked, and it is unambiguous which request it matched. Several
%% requests per side would make the model depend on the order in which cells
%% are handed out, which is a scheduler detail rather than a promise; the
%% stress suite covers that case with weaker, global invariants.

-module(prop_cbroker_statem).

-include_lib("proper/include/proper.hrl").

%% ------------------------------------------------------------------
%% Property Exports
%% ------------------------------------------------------------------

-export([
    prop_broker_behaves_like_the_model/0
]).

%% ------------------------------------------------------------------
%% proper_statem Function Exports
%% ------------------------------------------------------------------

-export([
    initial_state/0,
    command/1,
    precondition/2,
    postcondition/3,
    next_state/3
]).

%% ------------------------------------------------------------------
%% Command Exports
%% ------------------------------------------------------------------

-export([
    nb_ask/2,
    dynamic_ask/2,
    ask_without_waiting/2,
    cancel/1,
    collect/1
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(REPLY_TIMEOUT_MS, 5_000).

-record(state, {
    % `none`, or the parked request as {AskResult, Offer}
    left = none,
    right = none,
    % requests whose counterpart matched them, awaiting collection:
    % [{AskResult, CounterOffer}]
    matched = [],
    % makes every generated offer distinguishable
    nr_of_offers = 0
}).

%% ------------------------------------------------------------------
%% Properties
%% ------------------------------------------------------------------

prop_broker_behaves_like_the_model() ->
    ?FORALL(
        Cmds,
        commands(?MODULE),
        begin
            setup(),
            {History, State, Result} = run_commands(?MODULE, Cmds),
            Leftovers = cleanup(State),

            ?WHENFAIL(
                io:format(
                    "History: ~p~nState: ~p~nResult: ~p~nLeftovers: ~p~n",
                    [History, State, Result, Leftovers]
                ),
                Result =:= ok andalso Leftovers =:= []
            )
        end
    ).

%% ------------------------------------------------------------------
%% proper_statem Function Definitions
%% ------------------------------------------------------------------

initial_state() ->
    #state{}.

command(State) ->
    frequency(
        [
            {3, {call, ?MODULE, nb_ask, [side(), offer(State)]}},
            {1, {call, ?MODULE, ask_without_waiting, [side(), offer(State)]}}
        ] ++
            [
                {6, {call, ?MODULE, dynamic_ask, [parkable_side(State), offer(State)]}}
             || parkable_sides(State) =/= []
            ] ++
            [
                {3, {call, ?MODULE, cancel, [cancellable(State)]}}
             || cancellable_results(State) =/= []
            ] ++
            [
                {4, {call, ?MODULE, collect, [collectable(State)]}}
             || State#state.matched =/= []
            ]
    ).

precondition(State, {call, _, dynamic_ask, [Side, _]}) ->
    parked(State, Side) =:= none;
precondition(State, {call, _, cancel, [AskResult]}) ->
    lists:member(AskResult, cancellable_results(State));
precondition(State, {call, _, collect, [AskResult]}) ->
    lists:keymember(AskResult, 1, State#state.matched);
precondition(_State, _Call) ->
    true.

postcondition(State, {call, _, nb_ask, [Side, _Offer]}, Result) ->
    case parked(State, opposite(Side)) of
        none ->
            matches_drop(Result, match_unavailable);
        %
        {_, CounterOffer} ->
            matches_match(Result, CounterOffer)
    end;
postcondition(State, {call, _, ask_without_waiting, [Side, _Offer]}, Result) ->
    case parked(State, opposite(Side)) of
        none ->
            % Nothing to match, so `ask/4` cancels its own request
            matches_drop(Result, timeout);
        %
        {_, CounterOffer} ->
            matches_match(Result, CounterOffer)
    end;
postcondition(State, {call, _, dynamic_ask, [Side, _Offer]}, Result) ->
    case parked(State, opposite(Side)) of
        none ->
            case Result of
                {await, Ticket} -> is_reference(Ticket);
                _ -> false
            end;
        %
        {_, CounterOffer} ->
            matches_match(Result, CounterOffer)
    end;
postcondition(State, {call, _, cancel, [AskResult]}, Result) ->
    case lists:keymember(AskResult, 1, State#state.matched) of
        true ->
            % Its counterpart got there first, so the reply is already on its
            % way and must still be collectable
            Result =:= too_late;
        %
        false ->
            case Result of
                {cancelled, SojournTime} -> is_plausible_sojourn(SojournTime);
                _ -> false
            end
    end;
postcondition(State, {call, _, collect, [AskResult]}, Result) ->
    {AskResult, CounterOffer} = lists:keyfind(AskResult, 1, State#state.matched),
    matches_match(Result, CounterOffer).

next_state(State, Result, {call, _, dynamic_ask, [Side, Offer]}) ->
    case parked(State, opposite(Side)) of
        none ->
            park(State, Side, {Result, Offer});
        %
        {CounterAskResult, _} ->
            matched(unpark(State, opposite(Side)), CounterAskResult, Offer)
    end;
next_state(State, _Result, {call, _, Ask, [Side, Offer]}) when
    Ask =:= nb_ask; Ask =:= ask_without_waiting
->
    case parked(State, opposite(Side)) of
        none ->
            State;
        %
        {CounterAskResult, _} ->
            matched(unpark(State, opposite(Side)), CounterAskResult, Offer)
    end;
next_state(State, _Result, {call, _, cancel, [AskResult]}) ->
    % Either it was parked and is now gone, or it had already matched and its
    % reply is still pending collection
    case lists:keymember(AskResult, 1, State#state.matched) of
        true -> State;
        false -> unpark_result(State, AskResult)
    end;
next_state(State, _Result, {call, _, collect, [AskResult]}) ->
    State#state{matched = lists:keydelete(AskResult, 1, State#state.matched)}.

%% ------------------------------------------------------------------
%% Command Function Definitions
%% ------------------------------------------------------------------

nb_ask(Side, Offer) ->
    cbroker:nb_ask(broker(), Side, Offer).

dynamic_ask(Side, Offer) ->
    cbroker:dynamic_ask(broker(), Side, Offer).

ask_without_waiting(Side, Offer) ->
    cbroker:ask(broker(), Side, Offer, 0).

cancel({await, Ticket}) ->
    cbroker:cancel(Ticket).

collect({await, Ticket}) ->
    receive
        {Ticket, Reply} ->
            Reply
    after ?REPLY_TIMEOUT_MS ->
        {no_reply_for, Ticket}
    end.

%% ------------------------------------------------------------------
%% Generators
%% ------------------------------------------------------------------

side() ->
    oneof([left, right]).

parkable_side(State) ->
    oneof(parkable_sides(State)).

offer(#state{nr_of_offers = NrOfOffers}) ->
    {offer, NrOfOffers + 1}.

cancellable(State) ->
    oneof(cancellable_results(State)).

collectable(#state{matched = Matched}) ->
    oneof([AskResult || {AskResult, _} <- Matched]).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

parkable_sides(State) ->
    [Side || Side <- [left, right], parked(State, Side) =:= none].

% Parked requests, plus those already matched: cancelling the latter is how
% the `too_late` path gets exercised
cancellable_results(#state{left = Left, right = Right, matched = Matched}) ->
    [AskResult || {AskResult, _} <- [Left, Right], AskResult =/= none] ++
        [AskResult || {AskResult, _} <- Matched].

opposite(left) -> right;
opposite(right) -> left.

parked(#state{left = Left}, left) -> Left;
parked(#state{right = Right}, right) -> Right.

park(State, left, Request) -> State#state{left = Request, nr_of_offers = next_nr(State)};
park(State, right, Request) -> State#state{right = Request, nr_of_offers = next_nr(State)}.

unpark(State, left) -> State#state{left = none};
unpark(State, right) -> State#state{right = none}.

unpark_result(#state{left = {AskResult, _}} = State, AskResult) -> State#state{left = none};
unpark_result(#state{right = {AskResult, _}} = State, AskResult) -> State#state{right = none};
unpark_result(State, _AskResult) -> State.

matched(State, CounterAskResult, Offer) ->
    State#state{
        matched = [{CounterAskResult, Offer} | State#state.matched],
        nr_of_offers = next_nr(State)
    }.

next_nr(#state{nr_of_offers = NrOfOffers}) ->
    NrOfOffers + 1.

matches_match(Result, ExpectedCounterOffer) ->
    case Result of
        {match, MatchRef, CounterOffer, SojournTime} ->
            is_reference(MatchRef) andalso
                CounterOffer =:= ExpectedCounterOffer andalso
                is_plausible_sojourn(SojournTime);
        %
        _ ->
            false
    end.

matches_drop(Result, ExpectedReason) ->
    case Result of
        {drop, Reason, SojournTime} ->
            Reason =:= ExpectedReason andalso is_plausible_sojourn(SojournTime);
        %
        _ ->
            false
    end.

is_plausible_sojourn(SojournTime) ->
    is_integer(SojournTime) andalso SojournTime >= 0 andalso SojournTime < 10_000_000_000.

%%

setup() ->
    _ = flush_mailbox(),
    erlang:put(broker, cbroker:new()),
    ok.

% The broker should be parking exactly the requests the model still has
% parked: no leaked cells, and none vanished either
cleanup(#state{} = State) ->
    Broker = broker(),
    PendingCells = [
        {BatchId, Cell}
     || Batch <- batches(Broker),
        {id, BatchId} <- Batch,
        {cells, Cells} <- Batch,
        Cell <- Cells,
        Cell =/= empty,
        Cell =/= matched,
        Cell =/= cancelled
    ],
    Expected = length([Side || Side <- [left, right], parked(State, Side) =/= none]),

    erlang:erase(broker),

    [
        {pending_cells, PendingCells, expected_amount, Expected}
     || length(PendingCells) =/= Expected
    ].

batches(Broker) ->
    {batches, Batches} = lists:keyfind(batches, 1, cbroker:debug_info(Broker)),
    Batches.

broker() ->
    erlang:get(broker).

flush_mailbox() ->
    receive
        Msg -> [Msg | flush_mailbox()]
    after 0 -> []
    end.
