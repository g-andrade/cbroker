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

%% Concurrency stress. Unlike the `proper_statem` model, which predicts every
%% outcome from one process, these cases assert global invariants over many
%% processes: matches pair up exactly, timeouts never swallow a match,
%% requests left behind by dead processes get reclaimed, and batches get
%% released even by schedulers that only ever ask on one lane.
%%
%% Sized by environment variables, so the defaults keep `make test` quick while
%% `make stress` (or CI) can run the same cases for much longer:
%%
%%   CBROKER_STRESS_PROCS_PER_LANE   (default 8)
%%   CBROKER_STRESS_ITERATIONS       (default 250, per process)

-module(cbroker_stress_SUITE).
-behaviour(ct_suite).

-include_lib("stdlib/include/assert.hrl").

%% ------------------------------------------------------------------
%% ct_suite Function Exports
%% ------------------------------------------------------------------

-export([
    suite/0,
    all/0,
    groups/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% ------------------------------------------------------------------
%% Test Case Function Exports
%% ------------------------------------------------------------------

-export([
    every_match_is_paired/1,
    timeouts_never_lose_a_match/1,
    killed_waiters_are_reclaimed/1,
    one_lane_schedulers_release_batches/1,
    lagging_lane_skips_batch_it_dropped/1,
    queue_limit_keeps_its_accounting/1,
    closing_never_loses_a_reply/1,
    shared_envs_never_mix_up_offers/1,
    brokers_leave_nothing_allocated/1
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(ASK_TIMEOUT_MS, 30_000).
-define(SETTLE_TIMEOUT_MS, 10_000).

%% ------------------------------------------------------------------
%% ct_suite Function Definitions
%% ------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 30}}].

all() ->
    [{group, GroupName} || {GroupName, _Options, _TestCases} <- groups()].

groups() ->
    [
        {
            _Name = stress,
            % Each case is concurrent in itself, so the cases run one at a time
            _Opts = [],
            _TestCases = [
                every_match_is_paired,
                timeouts_never_lose_a_match,
                killed_waiters_are_reclaimed,
                one_lane_schedulers_release_batches,
                lagging_lane_skips_batch_it_dropped,
                queue_limit_keeps_its_accounting,
                closing_never_loses_a_reply,
                shared_envs_never_mix_up_offers,
                brokers_leave_nothing_allocated
            ]
        }
    ].

init_per_testcase(_TestCase, Config) ->
    [{broker, cbroker:new()} | Config].

end_per_testcase(_TestCase, Config) ->
    Broker = broker(Config),
    ?assertEqual([], pending_cells(Broker)),
    ?assertEqual(0, queue_balance(Broker)),
    ?assertEqual([], flush_mailbox()),
    Config.

%% ------------------------------------------------------------------
%% Test Cases Function Definitions
%% ------------------------------------------------------------------

% Everyone asks with a generous timeout, so every ask must match, and every
% match must be seen by exactly one process per lane, with the offers crossed
every_match_is_paired(Config) ->
    Broker = broker(Config),

    Samples = run_workers(Broker, fun(Lane, Offer) ->
        cbroker:ask(Broker, Lane, Offer, ?ASK_TIMEOUT_MS)
    end),

    ?assertEqual([], [Sample || {_, _, Reply} = Sample <- Samples, not is_match(Reply)]),
    ?assertEqual(2 * procs_per_lane() * iterations(), length(Samples)),
    assert_matches_are_paired(Samples).

% Tiny timeouts make every ask race its own cancellation. A cancellation that
% loses the race must still yield the match, so the pairing holds regardless
timeouts_never_lose_a_match(Config) ->
    Broker = broker(Config),

    Samples = run_workers(Broker, fun(Lane, Offer) ->
        cbroker:ask(Broker, Lane, Offer, rand:uniform(4) - 1)
    end),

    UnexpectedDrops = [
        Sample
     || {_, _, Reply} = Sample <- Samples,
        not is_match(Reply),
        Reply =/= drop_of_reason(Reply, timeout)
    ],

    ?assertEqual([], UnexpectedDrops),
    assert_matches_are_paired([Sample || {_, _, Reply} = Sample <- Samples, is_match(Reply)]).

% A process that dies while parked must have its request reclaimed, and must
% never be handed a counterpart afterwards
killed_waiters_are_reclaimed(Config) ->
    Broker = broker(Config),
    Amount = procs_per_lane(),

    Waiters = [spawn(fun() -> park_forever(Broker, left, {offer, N}) end) || N <- seq(Amount)],
    ok = wait_until(fun() -> length(pending_cells(Broker)) =:= Amount end),

    lists:foreach(fun(Pid) -> exit(Pid, kill) end, Waiters),
    ok = wait_until(fun() -> pending_cells(Broker) =:= [] end),

    % Nothing parked any more, so there is nothing left to match against
    ?assertMatch({drop, match_not_found, _}, cbroker:nb_ask(Broker, right, late_offer)).

% One scheduler only ever asks on `left`, another only on `right`, so each
% one's tail for the other lane never moves by itself. The `right` side trails
% by a batch, so every batch is still unconsumed when the `left` tail leaves
% it. Consumed batches must be released regardless, or they pile up for as
% long as the broker runs
one_lane_schedulers_release_batches(Config) ->
    case erlang:system_info(schedulers) of
        1 ->
            {skip, "needs two schedulers"};
        %
        _ ->
            assert_one_lane_schedulers_release_batches(broker(Config))
    end.

assert_one_lane_schedulers_release_batches(Broker) ->
    CellsPerBatch = nr_of_cells_per_batch(Broker),
    Rounds = 20,

    Left = spawn_pinned(1, fun({enqueue, Amount}) ->
        lists:foreach(
            fun(N) -> {await, _} = cbroker:async_ask(Broker, left, {offer, N}) end,
            seq(Amount)
        )
    end),
    Right = spawn_pinned(2, fun({consume, Amount}) ->
        lists:foreach(
            fun(_) -> {match, _, _, _} = cbroker:nb_ask(Broker, right, counter_offer) end,
            seq(Amount)
        )
    end),

    call_pinned(Left, {enqueue, 2 * CellsPerBatch}),
    BatchCounts = [
        begin
            call_pinned(Right, {consume, CellsPerBatch}),
            call_pinned(Left, {enqueue, CellsPerBatch}),
            length(batches(Broker))
        end
     || _ <- seq(Rounds)
    ],
    call_pinned(Right, {consume, 2 * CellsPerBatch}),

    ct:log("Live batches per round: ~p", [BatchCounts]),
    stop_pinned(Left, (Rounds + 2) * CellsPerBatch),
    stop_pinned(Right, 0),
    ?assert(lists:max(BatchCounts) =< 4, BatchCounts).

% One scheduler parks `right` asks three batches' worth, then matches them from
% `left`. Consuming the last cell of the `left` tail's batch drops that batch
% from the local state while the tail still points at it, so the next `left` ask
% skips a batch it no longer holds, with the `right` tail already past the one
% that follows. That used to trip an assertion and abort the emulator
lagging_lane_skips_batch_it_dropped(Config) ->
    Broker = broker(Config),
    Parked = 3 * nr_of_cells_per_batch(Broker),

    Asker = spawn_pinned(1, fun
        ({park, Amount}) ->
            lists:foreach(
                fun(N) -> {await, _} = cbroker:async_ask(Broker, right, {offer, N}) end,
                seq(Amount)
            );
        ({consume, Amount}) ->
            lists:foreach(
                fun(_) -> {match, _, _, _} = cbroker:nb_ask(Broker, left, counter_offer) end,
                seq(Amount)
            )
    end),

    call_pinned(Asker, {park, Parked}),
    call_pinned(Asker, {consume, Parked}),
    stop_pinned(Asker, Parked).

% Asks on both lanes race a tight queue limit and their own tiny timeouts. Each
% one either matches, is refused for the lane being full, or times out; matches
% still pair up, and once everyone is done no weight is left in the balance
queue_limit_keeps_its_accounting(_Config) ->
    Broker = cbroker:new([{max_queue_len, 2}]),

    Samples = run_workers(Broker, fun(Lane, Offer) ->
        cbroker:ask(Broker, Lane, Offer, rand:uniform(4) - 1)
    end),

    UnexpectedReplies = [
        Sample
     || {_, _, Reply} = Sample <- Samples,
        not is_match(Reply),
        Reply =/= drop_of_reason(Reply, timeout),
        Reply =/= drop_of_reason(Reply, full_lane)
    ],
    ct:log("Refused for being full: ~b of ~b", [
        length([R || {_, _, R} <- Samples, R =:= drop_of_reason(R, full_lane)]),
        length(Samples)
    ]),

    ?assertEqual([], UnexpectedReplies),
    assert_matches_are_paired([Sample || {_, _, Reply} = Sample <- Samples, is_match(Reply)]),
    ?assertEqual([], pending_cells(Broker)),
    ?assertEqual(0, queue_balance(Broker)).

% The broker is closed under everyone's feet: each worker closes it on reaching
% the same, randomly picked, iteration, so the closes race each other as well
% as the asks. Each ask either matches, is dropped for the broker closing, or
% finds it closed already. One that parked just as the broker closed must still
% be told, or it would sit there until its timeout
closing_never_loses_a_reply(_Config) ->
    lists:foreach(fun(_) -> assert_closing_never_loses_a_reply() end, seq(20)).

assert_closing_never_loses_a_reply() ->
    Broker = cbroker:new(),
    CloseAt = rand:uniform(iterations()),

    Samples = run_workers(Broker, fun(Lane, {_, _, N} = Offer) ->
        N =:= CloseAt andalso (ok = cbroker:close(Broker)),
        try
            cbroker:ask(Broker, Lane, Offer, ?ASK_TIMEOUT_MS)
        catch
            error:closed -> closed
        end
    end),

    UnexpectedReplies = [
        Sample
     || {_, _, Reply} = Sample <- Samples,
        not is_match(Reply),
        Reply =/= drop_of_reason(Reply, closed),
        Reply =/= closed
    ],
    ct:log("Matched: ~b, dropped for closing: ~b, found it closed: ~b", [
        length([R || {_, _, R} <- Samples, is_match(R)]),
        length([R || {_, _, R} <- Samples, R =:= drop_of_reason(R, closed)]),
        length([R || {_, _, R} <- Samples, R =:= closed])
    ]),

    ?assertEqual([], UnexpectedReplies),
    assert_matches_are_paired([Sample || {_, _, Reply} = Sample <- Samples, is_match(Reply)]),
    ?assertEqual([], pending_cells(Broker)),
    ?assertEqual(0, queue_balance(Broker)).

% Offers of varying sizes go through envs that are being written to by one
% scheduler while others read from them, and that get cleared as soon as their
% last offer leaves. A one byte budget retires every env after one offer, a
% small one has them roll over all the time, and the default has them cleared
% and written to again. Each offer carries a payload derived from its own key,
% so one that got mixed up with another, or cleared too soon, shows
shared_envs_never_mix_up_offers(_Config) ->
    lists:foreach(fun assert_shared_envs_never_mix_up_offers/1, [1, 256, 16_384]).

assert_shared_envs_never_mix_up_offers(Budget) ->
    Broker = cbroker:new([{shared_env_budget, Budget}]),

    Samples = run_workers(Broker, fun(Lane, {_, _, N} = Key) ->
        case cbroker:ask(Broker, Lane, {Key, payload(N)}, ?ASK_TIMEOUT_MS) of
            {match, MatchRef, {{_, _, CounterN} = CounterKey, CounterPayload}, SojournTime} ->
                ?assertEqual(payload(CounterN), CounterPayload),
                {match, MatchRef, CounterKey, SojournTime};
            %
            Reply ->
                Reply
        end
    end),

    ?assertEqual([], [Sample || {_, _, Reply} = Sample <- Samples, not is_match(Reply)]),
    assert_matches_are_paired(Samples),
    ?assertEqual([], pending_cells(Broker)),
    ?assertEqual(0, queue_balance(Broker)).

% Brokers that have been worked hard enough to roll over batches must leave
% nothing behind once collected. This is what the cell and pool assertions
% cannot see: memory that is consistent, just unreachable
brokers_leave_nothing_allocated(_Config) ->
    case alloc_perfcounters() of
        unavailable ->
            {skip, "built without allocation counters; see `make test-sanitized`"};
        %
        Baseline ->
            assert_nothing_stays_allocated(Baseline)
    end.

assert_nothing_stays_allocated(Baseline) ->
    lists:foreach(
        fun(ChurnFun) ->
            {Pid, MonRef} = spawn_monitor(ChurnFun),
            receive
                {'DOWN', MonRef, process, Pid, normal} -> ok
            after ?ASK_TIMEOUT_MS -> ct:fail({churn_timed_out, Pid})
            end
        end,
        lists:append([
            [
                fun churn_through_batches/0,
                fun close_over_parked_batches/0,
                fun offer_brokers_to_themselves/0
            ]
         || _ <- seq(3)
        ])
    ),

    ok = wait_until(fun() -> settled(collect_garbage(), Baseline) end),
    ?assertEqual(Baseline, alloc_perfcounters()).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% Enough asks to allocate batches beyond the first, in a process of its own, so
% that dropping the broker leaves nothing referencing it
churn_through_batches() ->
    Broker = cbroker:new(),
    CellsPerBatch = nr_of_cells_per_batch(Broker),

    lists:foreach(
        fun(N) ->
            {await, Ticket} = cbroker:async_ask(Broker, left, {offer, N}),
            {match, _, _, _} = cbroker:nb_ask(Broker, right, counter_offer),
            receive
                {Ticket, {match, _, _, _}} -> ok
            after ?ASK_TIMEOUT_MS -> exit(no_reply)
            end
        end,
        seq(2 * CellsPerBatch)
    ).

% Batches' worth of parked asks, all of them dropped by closing the broker
close_over_parked_batches() ->
    Broker = cbroker:new(),
    CellsPerBatch = nr_of_cells_per_batch(Broker),

    Tickets = [
        Ticket
     || N <- seq(2 * CellsPerBatch),
        {await, Ticket} <- [cbroker:async_ask(Broker, left, {offer, N})]
    ],
    ok = cbroker:close(Broker),

    lists:foreach(
        fun(Ticket) ->
            receive
                {Ticket, {drop, closed, _}} -> ok
            after ?ASK_TIMEOUT_MS -> exit(no_reply)
            end
        end,
        Tickets
    ).

% An offer holding its own broker makes the env it is copied into keep that
% broker alive, so every way for a request to leave must let go of the copy.
% The last two are still parked when this process dies: one is reclaimed for
% its asker dying, the other for its broker's creator doing so
offer_brokers_to_themselves() ->
    Matched = cbroker:new(),
    {await, MatchedTicket} = cbroker:async_ask(Matched, left, {offered, Matched}),
    {match, _, {offered, Matched}, _} = cbroker:nb_ask(Matched, right, {offered, Matched}),
    receive
        {MatchedTicket, {match, _, {offered, Matched}, _}} -> ok
    after ?ASK_TIMEOUT_MS -> exit(no_reply)
    end,

    Cancelled = cbroker:new(),
    {await, CancelledTicket} = cbroker:async_ask(Cancelled, left, {offered, Cancelled}),
    {cancelled, _} = cbroker:cancel(CancelledTicket),

    Closed = cbroker:new(),
    {await, ClosedTicket} = cbroker:async_ask(Closed, left, {offered, Closed}),
    ok = cbroker:close(Closed),
    receive
        {ClosedTicket, {drop, closed, _}} -> ok
    after ?ASK_TIMEOUT_MS -> exit(no_reply)
    end,

    Abandoned = cbroker:new(),
    {await, _} = cbroker:async_ask(Abandoned, left, {offered, Abandoned}),

    Dependent = cbroker:new([depends_on_creator]),
    {await, _} = cbroker:async_ask(Dependent, left, {offered, Dependent}),
    ok.

alloc_perfcounters() ->
    cbroker_nif:alloc_perfcounters().

collect_garbage() ->
    lists:foreach(fun erlang:garbage_collect/1, processes()),
    alloc_perfcounters().

% Resource destructors run on collection, so the counters only settle once every
% reference is gone; anything below the baseline is another case's broker going
settled(Counters, Baseline) ->
    lists:all(
        fun({{Key, Value}, {Key, BaselineValue}}) -> Value =< BaselineValue end,
        lists:zip(Counters, Baseline)
    ).

% Half the processes on each lane, each running AskFun for every iteration.
% Returns one {Lane, Offer, Reply} sample per ask
run_workers(_Broker, AskFun) ->
    Parent = self(),
    Iterations = iterations(),

    Workers = [
        spawn_monitor(fun() -> Parent ! {self(), worker_samples(Lane, Iterations, AskFun)} end)
     || Lane <- [left, right], _ <- seq(procs_per_lane())
    ],

    lists:append([collect_samples(Pid, MonRef) || {Pid, MonRef} <- Workers]).

worker_samples(Lane, Iterations, AskFun) ->
    [
        begin
            Offer = {Lane, self(), N},
            {Lane, Offer, AskFun(Lane, Offer)}
        end
     || N <- seq(Iterations)
    ].

collect_samples(Pid, MonRef) ->
    receive
        {Pid, Samples} ->
            demonitor(MonRef, [flush]),
            Samples;
        %
        {'DOWN', MonRef, process, Pid, Reason} ->
            ct:fail({worker_died, Pid, Reason})
    after ?ASK_TIMEOUT_MS + ?SETTLE_TIMEOUT_MS ->
        ct:fail({worker_timed_out, Pid})
    end.

% Each match ref must show up exactly twice, once per lane, and each lane must
% have received the other's offer
assert_matches_are_paired(Samples) ->
    ByMatchRef = lists:foldl(
        fun({Lane, Offer, {match, MatchRef, CounterOffer, _}}, Acc) ->
            Sample = {Lane, Offer, CounterOffer},
            maps:update_with(MatchRef, fun(Pairs) -> [Sample | Pairs] end, [Sample], Acc)
        end,
        #{},
        Samples
    ),

    Unpaired = maps:filter(fun(_, Pairs) -> not is_pair(Pairs) end, ByMatchRef),
    ?assertEqual(#{}, Unpaired).

is_pair([{LaneA, OfferA, CounterOfferA}, {LaneB, OfferB, CounterOfferB}]) ->
    LaneA =/= LaneB andalso
        CounterOfferA =:= OfferB andalso
        CounterOfferB =:= OfferA;
is_pair(_Pairs) ->
    false.

% A process bound to one scheduler, running Fun for each call. Linked, so that
% its crash fails the case rather than time it out
spawn_pinned(Scheduler, Fun) ->
    spawn_opt(fun() -> serve_pinned(Fun) end, [{scheduler, Scheduler}, link]).

% Calls are tagged, as the mailbox also collects `{Ticket, Reply}` messages
serve_pinned(Fun) ->
    receive
        {pinned_call, From, {stop, ExpectedMatches}} ->
            From ! {pinned_reply, self(), receive_matches(ExpectedMatches)};
        %
        {pinned_call, From, Request} ->
            Fun(Request),
            From ! {pinned_reply, self(), ok},
            serve_pinned(Fun)
    end.

receive_matches(0) ->
    ok;
receive_matches(Amount) ->
    receive
        {_Ticket, {match, _, _, _}} -> receive_matches(Amount - 1)
    after ?SETTLE_TIMEOUT_MS -> {missing_matches, Amount}
    end.

call_pinned(Pid, Request) ->
    Pid ! {pinned_call, self(), Request},
    receive
        {pinned_reply, Pid, Reply} -> Reply
    after ?ASK_TIMEOUT_MS -> ct:fail({pinned_call_timed_out, Pid, Request})
    end.

% Stops the process once it has received every match it was owed
stop_pinned(Pid, ExpectedMatches) ->
    ?assertEqual(ok, call_pinned(Pid, {stop, ExpectedMatches})).

park_forever(Broker, Lane, Offer) ->
    {await, _} = cbroker:async_ask(Broker, Lane, Offer),
    receive
        never -> ok
    end.

%%

% Anything from nothing at all to a few hundred bytes, depending on N alone
payload(N) ->
    lists:duplicate(N rem 40, N).

is_match({match, _, _, _}) -> true;
is_match(_Reply) -> false.

drop_of_reason({drop, Reason, SojournTime}, Reason) -> {drop, Reason, SojournTime};
drop_of_reason(_Reply, _Reason) -> no_such_drop.

procs_per_lane() ->
    env_int("CBROKER_STRESS_PROCS_PER_LANE", 8).

iterations() ->
    env_int("CBROKER_STRESS_ITERATIONS", 250).

env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> list_to_integer(string:trim(Value))
    end.

seq(Amount) ->
    lists:seq(1, Amount).

wait_until(Fun) ->
    wait_until(Fun, erlang:monotonic_time(millisecond) + ?SETTLE_TIMEOUT_MS).

wait_until(Fun, Deadline) ->
    case Fun() of
        true ->
            ok;
        %
        false ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(10),
                    wait_until(Fun, Deadline);
                %
                false ->
                    {error, timeout}
            end
    end.

broker(Config) ->
    proplists:get_value(broker, Config).

% Cells still parking a request; `matched` and `cancelled` are spent sentinels
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

% Parked `right` asks minus parked `left` ones, plus any asks in flight
queue_balance(Broker) ->
    {stats, Stats} = lists:keyfind(stats, 1, cbroker:debug_info(Broker)),
    {queue_balance, Balance} = lists:keyfind(queue_balance, 1, Stats),
    Balance.

% Every live batch, as held by the global state
batches(Broker) ->
    {batches, Batches} = lists:keyfind(batches, 1, cbroker:debug_info(Broker)),
    Batches.

nr_of_cells_per_batch(Broker) ->
    {opts, Opts} = lists:keyfind(
        opts, 1, cbroker:debug_info(Broker)
    ),

    {cells_per_batch, CellsPerBatch} = lists:keyfind(
        cells_per_batch, 1, Opts
    ),

    CellsPerBatch.

flush_mailbox() ->
    receive
        Msg -> [Msg | flush_mailbox()]
    after 0 -> []
    end.
