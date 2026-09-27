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

-module(cbroker_tests_SUITE).
-behaviour(ct_suite).

-include_lib("stdlib/include/assert.hrl").

%% ------------------------------------------------------------------
%% ct_suite Function Exports
%% ------------------------------------------------------------------

-export([
    all/0,
    groups/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% ------------------------------------------------------------------
%% Test Case Function Exports
%% ------------------------------------------------------------------

-export([
    match_is_symmetric/1,
    match_refs_agree/1,
    offers_survive_the_exchange/1,
    sojourn_times_are_plausible/1,
    match_between_processes/1,
    %
    nb_ask_without_counterpart_drops/1,
    nb_ask_matches_a_waiting_request/1,
    dynamic_ask_awaits_then_matches/1,
    %
    reply_is_tagged_with_the_ticket/1,
    reply_is_tagged_with_the_given_ref/1
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

% Matches are local and instantaneous, so a sojourn time anywhere near this
% means the arithmetic is wrong rather than the machine being slow
-define(MAX_PLAUSIBLE_SOJOURN_NS, 10_000_000_000).

%% ------------------------------------------------------------------
%% ct_suite Function Definitions
%% ------------------------------------------------------------------

all() ->
    [{group, GroupName} || {GroupName, _Options, _TestCases} <- groups()].

groups() ->
    [
        {
            _Name = matching,
            _Opts = [parallel],
            _TestCases = [
                match_is_symmetric,
                match_refs_agree,
                offers_survive_the_exchange,
                sojourn_times_are_plausible,
                match_between_processes
            ]
        },
        {
            non_blocking,
            [parallel],
            [
                nb_ask_without_counterpart_drops,
                nb_ask_matches_a_waiting_request,
                dynamic_ask_awaits_then_matches
            ]
        },
        {
            replies,
            [parallel],
            [
                reply_is_tagged_with_the_ticket,
                reply_is_tagged_with_the_given_ref
            ]
        }
    ].

% A broker per test case, checked for leftovers once the case is done
init_per_testcase(_TestCase, Config) ->
    [{broker, cbroker:new()} | Config].

end_per_testcase(_TestCase, Config) ->
    Broker = broker(Config),
    ?assertEqual([], pending_cells(Broker)),
    ?assertEqual([], flush_mailbox()),
    Config.

%% ------------------------------------------------------------------
%% Test Cases Function Definitions
%% ------------------------------------------------------------------

match_is_symmetric(Config) ->
    Broker = broker(Config),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    ?assertMatch({match, _, offer_l, _}, cbroker:nb_ask(Broker, right, offer_r)),
    ?assertMatch({match, _, offer_r, _}, await(Ticket)).

match_refs_agree(Config) ->
    Broker = broker(Config),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    {match, MatchRef, _, _} = cbroker:nb_ask(Broker, right, offer_r),
    {match, CounterMatchRef, _, _} = await(Ticket),

    ?assertEqual(MatchRef, CounterMatchRef).

offers_survive_the_exchange(Config) ->
    Broker = broker(Config),

    lists:foreach(
        fun(Offer) ->
            {await, Ticket} = cbroker:async_ask(Broker, left, Offer),
            ?assertMatch({match, _, Offer, _}, cbroker:nb_ask(Broker, right, counter_offer)),
            ?assertMatch({match, _, counter_offer, _}, await(Ticket))
        end,
        offers()
    ).

sojourn_times_are_plausible(Config) ->
    Broker = broker(Config),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    {match, _, _, MatcherSojourn} = cbroker:nb_ask(Broker, right, offer_r),
    {match, _, _, WaiterSojourn} = await(Ticket),

    lists:foreach(
        fun(SojournTime) ->
            ?assert(SojournTime >= 0),
            ?assert(SojournTime < ?MAX_PLAUSIBLE_SOJOURN_NS)
        end,
        [MatcherSojourn, WaiterSojourn]
    ).

match_between_processes(Config) ->
    Broker = broker(Config),
    Parent = self(),

    Left = spawn_link(fun() -> Parent ! {self(), cbroker:ask(Broker, left, offer_l)} end),
    ?assertMatch({match, _, offer_l, _}, cbroker:ask(Broker, right, offer_r)),

    receive
        {Left, Result} ->
            ?assertMatch({match, _, offer_r, _}, Result)
    after 5_000 ->
        ct:fail({no_result_from, Left})
    end.

%%

nb_ask_without_counterpart_drops(Config) ->
    Broker = broker(Config),

    ?assertMatch({drop, match_unavailable, _}, cbroker:nb_ask(Broker, left, offer_l)).

nb_ask_matches_a_waiting_request(Config) ->
    Broker = broker(Config),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    ?assertMatch({match, _, offer_l, _}, cbroker:nb_ask(Broker, right, offer_r)),
    ?assertMatch({match, _, offer_r, _}, await(Ticket)).

dynamic_ask_awaits_then_matches(Config) ->
    Broker = broker(Config),

    {await, Ticket} = cbroker:dynamic_ask(Broker, left, offer_l),
    ?assertMatch({match, _, offer_l, _}, cbroker:dynamic_ask(Broker, right, offer_r)),
    ?assertMatch({match, _, offer_r, _}, await(Ticket)).

%%

reply_is_tagged_with_the_ticket(Config) ->
    Broker = broker(Config),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    {match, _, _, _} = cbroker:nb_ask(Broker, right, offer_r),

    receive
        {Tag, Reply} ->
            ?assertEqual(Ticket, Tag),
            ?assertMatch({match, _, offer_r, _}, Reply)
    after 5_000 ->
        ct:fail(no_reply)
    end.

reply_is_tagged_with_the_given_ref(Config) ->
    Broker = broker(Config),
    ReplyRef = make_ref(),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l, ReplyRef),
    {match, _, _, _} = cbroker:nb_ask(Broker, right, offer_r),

    receive
        {Tag, Reply} ->
            ?assertEqual(ReplyRef, Tag),
            ?assertNotEqual(Ticket, Tag),
            ?assertMatch({match, _, offer_r, _}, Reply)
    after 5_000 ->
        ct:fail(no_reply)
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

broker(Config) ->
    proplists:get_value(broker, Config).

await(Ticket) ->
    receive
        {Ticket, Reply} ->
            Reply
    after 5_000 ->
        ct:fail({no_reply_for, Ticket})
    end.

% Offers of assorted shapes and sizes: immediates, heap binaries, refc
% binaries, and terms large enough to matter to the offer-size accounting
offers() ->
    [
        an_atom,
        42,
        -42,
        {self(), make_ref()},
        binary:copy(<<"small">>, 4),
        binary:copy(<<"large">>, 1_000),
        #{nested => [1, 2, 3, #{deeper => <<"value">>}]},
        list_to_tuple(lists:seq(1, 1_000))
    ].

% Cells still parking a request. `matched` and `cancelled` cells are spent
% sentinels, but nothing should be left `waiting` once a test case is done
pending_cells(Broker) ->
    {batches, Batches} = lists:keyfind(batches, 1, cbroker:debug_info(Broker)),
    [
        {BatchId, Cell}
     || Batch <- Batches,
        {id, BatchId} <- Batch,
        {cells, Cells} <- Batch,
        Cell <- Cells,
        Cell =/= empty,
        Cell =/= matched,
        Cell =/= cancelled
    ].

flush_mailbox() ->
    receive
        Msg ->
            [Msg | flush_mailbox()]
    after 0 ->
        []
    end.
