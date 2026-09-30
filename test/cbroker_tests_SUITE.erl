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
    reply_is_tagged_with_the_given_ref/1,
    %
    defaults_are_reported/1,
    depends_on_creator_closes_the_broker/1,
    broker_outlives_its_creator_by_default/1,
    cells_per_batch_sizes_batches/1,
    ask_credits_and_max_tries_bound_an_ask/1,
    pools_start_with_their_initial_count/1,
    pool_opts_can_be_given_alone/1,
    request_pool_size_caps_what_it_keeps/1,
    invalid_opts_are_rejected/1
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
        },
        {
            opts,
            [parallel],
            [
                defaults_are_reported,
                depends_on_creator_closes_the_broker,
                broker_outlives_its_creator_by_default,
                cells_per_batch_sizes_batches,
                ask_credits_and_max_tries_bound_an_ask,
                pools_start_with_their_initial_count,
                pool_opts_can_be_given_alone,
                request_pool_size_caps_what_it_keeps,
                invalid_opts_are_rejected
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

%%

defaults_are_reported(_Config) ->
    Broker = cbroker:new(),
    Schedulers = debug_value(schedulers, Broker),

    ?assertEqual(
        [
            {depends_on_creator, false},
            {cells_per_batch, 32 * Schedulers},
            {ask_credits, 400},
            {ask_max_tries, 10},
            {batch_pool, [{size, 4}, {initial_count, 1}]},
            {request_pool, [{size, 8}, {initial_count, 0}]},
            {ticket_pool, [{size, 8}, {initial_count, 0}]}
        ],
        debug_value(opts, Broker)
    ).

depends_on_creator_closes_the_broker(_Config) ->
    {Broker, Creator} = broker_with_creator([depends_on_creator]),
    ?assertEqual(true, opt(depends_on_creator, Broker)),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    stop_creator(Creator),

    ?assertMatch({drop, broker_closed, _}, await(Ticket)),
    ?assertError(broker_closed, cbroker:nb_ask(Broker, right, offer_r)).

broker_outlives_its_creator_by_default(_Config) ->
    {Broker, Creator} = broker_with_creator([]),
    ?assertEqual(false, opt(depends_on_creator, Broker)),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    stop_creator(Creator),

    ?assertMatch({match, _, offer_l, _}, cbroker:nb_ask(Broker, right, offer_r)),
    ?assertMatch({match, _, offer_r, _}, await(Ticket)).

% One ask past a full batch rolls over to a second one of the same size
cells_per_batch_sizes_batches(_Config) ->
    Broker = cbroker:new([{cells_per_batch, 4}]),
    ?assertEqual(4, opt(cells_per_batch, Broker)),

    Tickets = [T || N <- lists:seq(1, 5), {await, T} <- [cbroker:async_ask(Broker, left, N)]],
    ?assertEqual([4, 4], [length(Cells) || Batch <- batches(Broker), {cells, Cells} <- Batch]),

    lists:foreach(
        fun(_) -> {match, _, _, _} = cbroker:nb_ask(Broker, right, counter_offer) end,
        Tickets
    ),
    lists:foreach(fun(T) -> ?assertMatch({match, _, counter_offer, _}, await(T)) end, Tickets),
    ?assertEqual([], pending_cells(Broker)).

% A cancelled cell ahead of the ask costs one credit. With a single credit and a
% single try, that's all the ask gets; one more of either reaches the next cell
ask_credits_and_max_tries_bound_an_ask(_Config) ->
    lists:foreach(
        fun({Opts, ExpectedReason}) ->
            Broker = cbroker:new(Opts),
            {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
            {cancelled, _} = cbroker:cancel(Ticket),

            ?assertMatch(
                {_, {drop, ExpectedReason, _}},
                {Opts, cbroker:nb_ask(Broker, right, offer_r)}
            )
        end,
        [
            {[{ask_credits, 1}, {ask_max_tries, 1}], broker_overloaded},
            {[{ask_credits, 1}, {ask_max_tries, 2}], match_unavailable},
            {[{ask_credits, 2}, {ask_max_tries, 1}], match_unavailable}
        ]
    ).

pools_start_with_their_initial_count(_Config) ->
    Broker = cbroker:new([
        {batch_pool, [{size, 6}, {initial_count, 5}]},
        {request_pool, [{size, 3}, {initial_count, 2}]},
        {ticket_pool, [{size, 4}, {initial_count, 4}]}
    ]),

    ?assertEqual([{size, 6}, {initial_count, 5}], opt(batch_pool, Broker)),
    ?assertEqual([{size, 3}, {initial_count, 2}], opt(request_pool, Broker)),
    ?assertEqual([{size, 4}, {initial_count, 4}], opt(ticket_pool, Broker)),

    {batch_pool, BatchPoolCount} = lists:keyfind(
        batch_pool, 1, debug_value(global_state, Broker)
    ),
    ?assertEqual(5, BatchPoolCount),
    ?assertEqual([2], lists:usort(local_pool_counts(request_pool, Broker))),
    ?assertEqual([4], lists:usort(local_pool_counts(ticket_pool, Broker))).

% Only `size` clamps the default initial count to it; only `initial_count`
% grows the default size to fit it
pool_opts_can_be_given_alone(_Config) ->
    lists:foreach(
        fun({Pool, PoolOpts, Expected}) ->
            Broker = cbroker:new([{Pool, PoolOpts}]),
            ?assertEqual({Pool, PoolOpts, Expected}, {Pool, PoolOpts, opt(Pool, Broker)})
        end,
        [
            {batch_pool, [{size, 0}], [{size, 0}, {initial_count, 0}]},
            {batch_pool, [{size, 3}], [{size, 3}, {initial_count, 1}]},
            {batch_pool, [{initial_count, 2}], [{size, 4}, {initial_count, 2}]},
            {request_pool, [{initial_count, 12}], [{size, 12}, {initial_count, 12}]},
            {ticket_pool, [], [{size, 8}, {initial_count, 0}]}
        ]
    ).

% Cancelling returns each request to the canceller's scheduler's pool, which
% keeps no more than `size` of them. A size of 0 keeps none, but still works
request_pool_size_caps_what_it_keeps(_Config) ->
    Parked = 5,

    lists:foreach(
        fun(Size) ->
            Broker = cbroker:new([{request_pool, [{size, Size}]}]),

            on_scheduler(1, fun() ->
                Tickets = [
                    T
                 || N <- lists:seq(1, Parked), {await, T} <- [cbroker:async_ask(Broker, left, N)]
                ],
                lists:foreach(fun(T) -> {cancelled, _} = cbroker:cancel(T) end, Tickets)
            end),

            ?assertEqual(
                {Size, min(Size, Parked)},
                {Size, lists:sum(local_pool_counts(request_pool, Broker))}
            ),
            ?assertEqual([], pending_cells(Broker))
        end,
        [0, 2, 8]
    ).

invalid_opts_are_rejected(_Config) ->
    NotAnOpt = [42, "depends_on_creator", {}, {depends_on_creator}, {depends_on_creator, true, x}],
    NonAtomKey = [{"depends_on_creator", true}, {1, true}],
    UnknownKey = [unknown_opt, {unknown_opt, true}],
    % A bare atom stands for `{Atom, true}`, which only a boolean opt accepts
    BareNonBoolean = [cells_per_batch, ask_credits, ask_max_tries, batch_pool],
    BadValue = [
        {depends_on_creator, sometimes},
        {depends_on_creator, 1},
        {cells_per_batch, 0},
        {cells_per_batch, -1},
        {cells_per_batch, 4.0},
        {cells_per_batch, 1 bsl 64},
        {ask_credits, 0},
        {ask_credits, -1},
        {ask_credits, many},
        {ask_credits, 1 bsl 31},
        {ask_max_tries, 0},
        {ask_max_tries, -1},
        {ask_max_tries, many}
    ],
    BadPoolOpts = [
        not_a_list,
        [{size, 1} | improper],
        [size],
        [{size}],
        [{size, 1, 2}],
        [{"size", 1}],
        [{bogus, 1}],
        [{size, -1}],
        [{size, many}],
        [{size, 1 bsl 64}],
        [{initial_count, -1}],
        [{initial_count, many}],
        [{size, 2}, {initial_count, 5}],
        [{initial_count, 5}, {size, 2}]
    ],
    BadPool = [
        {Pool, PoolOpts}
     || Pool <- [batch_pool, request_pool, ticket_pool], PoolOpts <- BadPoolOpts
    ],

    lists:foreach(
        fun(Opt) -> ?assertError({badopt, Opt}, cbroker:new([Opt])) end,
        NotAnOpt ++ NonAtomKey ++ UnknownKey ++ BareNonBoolean ++ BadValue ++ BadPool
    ),

    % Anything after a bad opt isn't looked at
    ?assertError({badopt, unknown_opt}, cbroker:new([depends_on_creator, unknown_opt, 42])),

    ?assertError({badopts, not_a_list}, cbroker:new(not_a_list)),
    ?assertError({badopts, improper}, cbroker:new([depends_on_creator | improper])).

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
    [
        {BatchId, Cell}
     || Batch <- batches(Broker),
        {id, BatchId} <- Batch,
        {cells, Cells} <- Batch,
        Cell <- Cells,
        Cell =/= empty,
        Cell =/= matched,
        Cell =/= cancelled
    ].

% Every live batch, as held by the global state
batches(Broker) ->
    debug_value(batches, Broker).

debug_value(Key, Broker) ->
    {Key, Value} = lists:keyfind(Key, 1, cbroker:debug_info(Broker)),
    Value.

opt(Key, Broker) ->
    {Key, Value} = lists:keyfind(Key, 1, debug_value(opts, Broker)),
    Value.

% One count per scheduler; which one is which depends on which asked first
local_pool_counts(Pool, Broker) ->
    [
        Count
     || LocalState <- debug_value(local_states, Broker), {P, Count} <- LocalState, P =:= Pool
    ].

% A broker created by a process of its own, which lives until `stop_creator/1`
broker_with_creator(Opts) ->
    Parent = self(),
    Creator = spawn(fun() ->
        Parent ! {self(), cbroker:new(Opts)},
        receive
            stop -> ok
        end
    end),
    receive
        {Creator, Broker} -> {Broker, Creator}
    after 5_000 ->
        ct:fail({no_broker_from, Creator})
    end.

stop_creator(Creator) ->
    MonRef = monitor(process, Creator),
    Creator ! stop,
    receive
        {'DOWN', MonRef, process, Creator, _} -> ok
    end.

% Runs Fun to completion in a process bound to Scheduler, so that every ask it
% makes goes through the same local state
on_scheduler(Scheduler, Fun) ->
    {Pid, MonRef} = spawn_opt(Fun, [{scheduler, Scheduler}, monitor]),
    receive
        {'DOWN', MonRef, process, Pid, normal} -> ok;
        {'DOWN', MonRef, process, Pid, Reason} -> ct:fail({pinned_process_died, Reason})
    after 5_000 ->
        ct:fail({pinned_process_timed_out, Pid})
    end.

flush_mailbox() ->
    receive
        Msg ->
            [Msg | flush_mailbox()]
    after 0 ->
        []
    end.
