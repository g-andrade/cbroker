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
    async_match_messages_the_right_lane_first/1,
    %
    defaults_are_reported/1,
    depends_on_creator_closes_the_broker/1,
    broker_outlives_its_creator_by_default/1,
    cells_per_batch_sizes_batches/1,
    ask_credits_and_max_tries_bound_an_ask/1,
    pools_start_with_their_initial_count/1,
    pool_opts_can_be_given_alone/1,
    request_pool_size_caps_what_it_keeps/1,
    invalid_opts_are_rejected/1,
    %
    closing_drops_waiters_and_refuses_asks/1,
    closing_again_changes_nothing/1,
    any_process_may_close/1,
    creator_may_die_after_closing/1,
    cancelling_after_closing_is_too_late/1,
    %
    queue_limit_opts_are_reported/1,
    queue_balance_follows_waiters/1,
    queue_balance_settles_after_every_outcome/1,
    full_lane_refuses_every_flavour/1,
    full_lane_still_matches_the_other/1,
    room_comes_back_once_a_waiter_leaves/1,
    one_sided_limits_leave_the_other_lane_alone/1,
    asymmetric_limits_bound_each_lane_apart/1,
    %
    default_offer_is_the_asker/1,
    resumable_ask_returns_a_ready_match/1,
    resumable_ask_waits_for_a_match/1,
    resumable_ask_stays_enqueued_on_timeout/1,
    %
    child_spec_defaults_to_no_opts/1,
    named_broker_is_asked_by_name/1,
    named_broker_takes_opts/1,
    stopping_closes_and_forgets_the_name/1,
    closing_by_name_keeps_the_name/1,
    crashing_keeps_the_name_until_restarted/1,
    unknown_and_invalid_names_are_rejected/1,
    named_server_survives_what_it_does_not_handle/1
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
                reply_is_tagged_with_the_given_ref,
                async_match_messages_the_right_lane_first
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
        },
        {
            closing,
            [parallel],
            [
                closing_drops_waiters_and_refuses_asks,
                closing_again_changes_nothing,
                any_process_may_close,
                creator_may_die_after_closing,
                cancelling_after_closing_is_too_late
            ]
        },
        {
            queue_limits,
            [parallel],
            [
                queue_limit_opts_are_reported,
                queue_balance_follows_waiters,
                queue_balance_settles_after_every_outcome,
                full_lane_refuses_every_flavour,
                full_lane_still_matches_the_other,
                room_comes_back_once_a_waiter_leaves,
                one_sided_limits_leave_the_other_lane_alone,
                asymmetric_limits_bound_each_lane_apart
            ]
        },
        {
            defaults_and_resuming,
            [parallel],
            [
                default_offer_is_the_asker,
                resumable_ask_returns_a_ready_match,
                resumable_ask_waits_for_a_match,
                resumable_ask_stays_enqueued_on_timeout
            ]
        },
        {
            named,
            [parallel],
            [
                child_spec_defaults_to_no_opts,
                named_broker_is_asked_by_name,
                named_broker_takes_opts,
                stopping_closes_and_forgets_the_name,
                closing_by_name_keeps_the_name,
                crashing_keeps_the_name_until_restarted,
                unknown_and_invalid_names_are_rejected,
                named_server_survives_what_it_does_not_handle
            ]
        }
    ].

% A broker per test case, checked for leftovers once the case is done
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

    ?assertMatch({drop, match_not_found, _}, cbroker:nb_ask(Broker, left, offer_l)).

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

% When the matcher is an `async_ask`, both sides are told by message. Being
% both sides ourselves, we get to see the order: `right` always comes first,
% whichever lane waited and whatever each side is tagged with
async_match_messages_the_right_lane_first(Config) ->
    Broker = broker(Config),
    TagKinds = [ticket, reply_ref],

    lists:foreach(
        fun({WaiterLane, WaiterTagKind, MatcherTagKind} = Combo) ->
            MatcherLane = other_lane(WaiterLane),
            WaiterReplyRef = reply_ref_arg(WaiterTagKind),
            MatcherReplyRef = reply_ref_arg(MatcherTagKind),

            {await, WaiterTicket} = cbroker:async_ask(
                Broker, WaiterLane, {offer, WaiterLane}, WaiterReplyRef
            ),
            {await, MatcherTicket} = cbroker:async_ask(
                Broker, MatcherLane, {offer, MatcherLane}, MatcherReplyRef
            ),
            Tags = #{
                WaiterLane => expected_tag(WaiterReplyRef, WaiterTicket),
                MatcherLane => expected_tag(MatcherReplyRef, MatcherTicket)
            },

            {FirstTag, {match, MatchRef, FirstCounterOffer, _}} = next_message(),
            {SecondTag, {match, MatchRef, SecondCounterOffer, _}} = next_message(),

            ?assertEqual(
                {Combo, maps:get(right, Tags), {offer, left}},
                {Combo, FirstTag, FirstCounterOffer}
            ),
            ?assertEqual(
                {Combo, maps:get(left, Tags), {offer, right}},
                {Combo, SecondTag, SecondCounterOffer}
            )
        end,
        [
            {WaiterLane, WaiterTagKind, MatcherTagKind}
         || WaiterLane <- [left, right],
            WaiterTagKind <- TagKinds,
            MatcherTagKind <- TagKinds
        ]
    ).

%%

defaults_are_reported(_Config) ->
    Broker = cbroker:new(),
    Schedulers = debug_value(schedulers, Broker),

    ?assertEqual(
        [
            {depends_on_creator, false},
            {cells_per_batch, 32 * Schedulers},
            {min_left_balance, unlimited},
            {max_right_balance, unlimited},
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

    ?assertMatch({drop, closed, _}, await(Ticket)),
    ?assertError(closed, cbroker:nb_ask(Broker, right, offer_r)).

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
            {[{ask_credits, 1}, {ask_max_tries, 1}], too_many_tries},
            {[{ask_credits, 1}, {ask_max_tries, 2}], match_not_found},
            {[{ask_credits, 2}, {ask_max_tries, 1}], match_not_found}
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
    BareNonBoolean = [
        cells_per_batch,
        max_queue_len,
        min_left_balance,
        max_right_balance,
        ask_credits,
        ask_max_tries,
        batch_pool
    ],
    BadValue = [
        {depends_on_creator, sometimes},
        {depends_on_creator, 1},
        {cells_per_batch, 0},
        {cells_per_batch, -1},
        {cells_per_batch, 4.0},
        {cells_per_batch, 1 bsl 64},
        % A limit of 0 would let nothing wait, and so nothing match
        {max_queue_len, 0},
        {max_queue_len, -1},
        {max_queue_len, 1 bsl 63},
        {max_queue_len, 1 bsl 64},
        {max_queue_len, 1.0},
        {max_queue_len, infinity},
        {min_left_balance, 0},
        {min_left_balance, 1},
        {min_left_balance, -(1 bsl 63) - 1},
        {min_left_balance, -1.0},
        {min_left_balance, infinity},
        {max_right_balance, 0},
        {max_right_balance, -1},
        {max_right_balance, 1 bsl 63},
        {max_right_balance, 1.0},
        {max_right_balance, infinity},
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

%%

closing_drops_waiters_and_refuses_asks(_Config) ->
    Broker = cbroker:new(),
    Lefts = park(Broker, left, 3),
    ?assertEqual(ok, cbroker:close(Broker)),

    lists:foreach(fun(Ticket) -> ?assertMatch({drop, closed, _}, await(Ticket)) end, Lefts),
    ?assertEqual([], pending_cells(Broker)),
    ?assertEqual(0, queue_balance(Broker)),

    ?assertError(closed, cbroker:nb_ask(Broker, right, offer_r)),
    ?assertError(closed, cbroker:async_ask(Broker, left, offer_l)),
    ?assertError(closed, cbroker:ask(Broker, right, offer_r)).

closing_again_changes_nothing(_Config) ->
    Broker = cbroker:new(),
    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),

    ?assertEqual(ok, cbroker:close(Broker)),
    ?assertEqual(ok, cbroker:close(Broker)),

    % A single reply: the case ends with an empty mailbox
    ?assertMatch({drop, closed, _}, await(Ticket)),
    ?assertError(closed, cbroker:nb_ask(Broker, right, offer_r)).

% Closing is not reserved to the creator, whether the broker depends on it
% or not
any_process_may_close(_Config) ->
    lists:foreach(
        fun(Opts) ->
            {Broker, Creator} = broker_with_creator(Opts),
            {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),

            ?assertEqual(ok, cbroker:close(Broker)),

            ?assertMatch({drop, closed, _}, await(Ticket)),
            ?assertError(closed, cbroker:nb_ask(Broker, right, offer_r)),
            stop_creator(Creator)
        end,
        [[], [depends_on_creator]]
    ).

% The creator's death finds the broker closed already, and leaves it be
creator_may_die_after_closing(_Config) ->
    {Broker, Creator} = broker_with_creator([depends_on_creator]),
    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),

    ?assertEqual(ok, cbroker:close(Broker)),
    ?assertMatch({drop, closed, _}, await(Ticket)),
    stop_creator(Creator),

    ?assertError(closed, cbroker:nb_ask(Broker, right, offer_r)),
    ?assertEqual(ok, cbroker:close(Broker)).

cancelling_after_closing_is_too_late(_Config) ->
    Broker = cbroker:new(),
    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    ?assertEqual(ok, cbroker:close(Broker)),

    ?assertEqual(too_late, cbroker:cancel(Ticket)),
    ?assertMatch({drop, closed, _}, await(Ticket)).

%%

% `max_queue_len` sets both bounds at once; whichever opt comes last wins
queue_limit_opts_are_reported(_Config) ->
    PtrdiffMax = (1 bsl 63) - 1,

    lists:foreach(
        fun({Opts, Expected}) ->
            Broker = cbroker:new(Opts),
            Reported = {opt(min_left_balance, Broker), opt(max_right_balance, Broker)},
            ?assertEqual({Opts, Expected}, {Opts, Reported})
        end,
        [
            {[{max_queue_len, 1}], {-1, 1}},
            {[{max_queue_len, 3}], {-3, 3}},
            {[{max_queue_len, PtrdiffMax}], {-PtrdiffMax, PtrdiffMax}},
            {[{max_queue_len, unlimited}], {unlimited, unlimited}},
            {[{min_left_balance, -2}], {-2, unlimited}},
            {[{max_right_balance, 5}], {unlimited, 5}},
            {[{min_left_balance, -2}, {max_right_balance, 5}], {-2, 5}},
            {[{min_left_balance, -(1 bsl 63)}], {-(1 bsl 63), unlimited}},
            {[{max_right_balance, PtrdiffMax}], {unlimited, PtrdiffMax}},
            {[{max_queue_len, 3}, {min_left_balance, -1}], {-1, 3}},
            {[{min_left_balance, -1}, {max_queue_len, 3}], {-3, 3}},
            {[{max_queue_len, 3}, {max_right_balance, unlimited}], {-3, unlimited}},
            {[{max_queue_len, 3}, {max_queue_len, unlimited}], {unlimited, unlimited}},
            {[{min_left_balance, -2}, {min_left_balance, unlimited}], {unlimited, unlimited}}
        ]
    ).

% Parked `left` asks count -1 each, parked `right` ones +1, and whatever takes
% a waiter out of its cell takes its weight back
queue_balance_follows_waiters(_Config) ->
    Broker = cbroker:new(),
    ?assertEqual(0, queue_balance(Broker)),

    [T1, T2, T3] = park(Broker, left, 3),
    ?assertEqual(-3, queue_balance(Broker)),

    {match, _, _, _} = cbroker:nb_ask(Broker, right, offer_r),
    ?assertMatch({match, _, _, _}, await(T1)),
    ?assertEqual(-2, queue_balance(Broker)),

    {cancelled, _} = cbroker:cancel(T2),
    ?assertEqual(-1, queue_balance(Broker)),

    {match, _, _, _} = cbroker:ask(Broker, right, offer_r),
    ?assertMatch({match, _, _, _}, await(T3)),
    ?assertEqual(0, queue_balance(Broker)),

    Parker = park_in_process(Broker, right),
    ?assertEqual(1, queue_balance(Broker)),
    exit(Parker, kill),
    wait_until(fun() -> queue_balance(Broker) =:= 0 end).

% Asks that end without parking leave nothing behind
queue_balance_settles_after_every_outcome(_Config) ->
    NoMatch = cbroker:new(),
    ?assertMatch({drop, match_not_found, _}, cbroker:nb_ask(NoMatch, left, offer_l)),
    ?assertEqual(0, queue_balance(NoMatch)),

    % Out of tries on the cancelled cell ahead. Being async, it's told by message
    Overloaded = cbroker:new([{ask_credits, 1}, {ask_max_tries, 1}]),
    [Cancelled] = park(Overloaded, left, 1),
    {cancelled, _} = cbroker:cancel(Cancelled),
    {await, Ticket} = cbroker:async_ask(Overloaded, right, offer_r),
    ?assertMatch({drop, too_many_tries, _}, await(Ticket)),
    ?assertEqual(0, queue_balance(Overloaded)),

    {Closed, Creator} = broker_with_creator([depends_on_creator]),
    [Parked] = park(Closed, right, 1),
    stop_creator(Creator),
    ?assertMatch({drop, closed, _}, await(Parked)),
    ?assertEqual(0, queue_balance(Closed)).

% Once a lane has as many waiters as allowed, one more is refused however it
% asks, and nothing about the waiters changes. `nb_ask` wouldn't wait anyway
full_lane_refuses_every_flavour(_Config) ->
    lists:foreach(
        fun(Lane) ->
            Broker = cbroker:new([{max_queue_len, 2}]),
            Parked = park(Broker, Lane, 2),
            Balance = queue_balance(Broker),

            ?assertMatch(
                {drop, full_lane, SojournTime} when SojournTime >= 0,
                cbroker:ask(Broker, Lane, offer, 5_000)
            ),
            ?assertMatch({drop, full_lane, _}, cbroker:dynamic_ask(Broker, Lane, offer)),

            {await, Ticket} = cbroker:async_ask(Broker, Lane, offer),
            ?assertMatch({drop, full_lane, _}, await(Ticket)),

            ReplyRef = make_ref(),
            ?assertEqual({await, ReplyRef}, cbroker:async_ask(Broker, Lane, offer, ReplyRef)),
            ?assertMatch({drop, full_lane, _}, await(ReplyRef)),

            ?assertMatch({drop, match_not_found, _}, cbroker:nb_ask(Broker, Lane, offer)),

            ?assertEqual(Balance, queue_balance(Broker)),
            drain(Broker, other_lane(Lane), Parked)
        end,
        [left, right]
    ).

% An ask that takes a waiter only brings the balance closer to zero, so a full
% lane never stops the other one from matching
full_lane_still_matches_the_other(_Config) ->
    lists:foreach(
        fun(Lane) ->
            Other = other_lane(Lane),
            Broker = cbroker:new([{max_queue_len, 4}]),
            Parked = park(Broker, Lane, 4),

            ?assertMatch({match, _, _, _}, cbroker:ask(Broker, Other, offer, 5_000)),
            ?assertMatch({match, _, _, _}, cbroker:dynamic_ask(Broker, Other, offer)),
            {await, Ticket} = cbroker:async_ask(Broker, Other, offer),
            ?assertMatch({match, _, _, _}, await(Ticket)),
            ?assertMatch({match, _, _, _}, cbroker:nb_ask(Broker, Other, offer)),

            lists:foreach(fun(T) -> ?assertMatch({match, _, offer, _}, await(T)) end, Parked),
            ?assertEqual(0, queue_balance(Broker))
        end,
        [left, right]
    ).

% With room for a single waiter, each way of leaving frees it for the next
room_comes_back_once_a_waiter_leaves(_Config) ->
    Broker = cbroker:new([{max_queue_len, 1}]),
    AssertFull = fun() ->
        ?assertMatch({drop, full_lane, _}, cbroker:dynamic_ask(Broker, left, offer))
    end,

    [Matched] = park(Broker, left, 1),
    AssertFull(),
    drain(Broker, right, [Matched]),

    [Cancelled] = park(Broker, left, 1),
    AssertFull(),
    {cancelled, _} = cbroker:cancel(Cancelled),

    Parker = park_in_process(Broker, left),
    AssertFull(),
    exit(Parker, kill),
    wait_until(fun() -> queue_balance(Broker) =:= 0 end),

    drain(Broker, right, park(Broker, left, 1)).

% A bound on one side only: that lane holds a single waiter, the other as many
% as it likes, and each can still drain the other
one_sided_limits_leave_the_other_lane_alone(_Config) ->
    lists:foreach(
        fun({Opts, Limited}) ->
            Unlimited = other_lane(Limited),
            Broker = cbroker:new(Opts),

            Parked = park(Broker, Limited, 1),
            ?assertMatch({drop, full_lane, _}, cbroker:dynamic_ask(Broker, Limited, offer)),
            drain(Broker, Unlimited, Parked),

            drain(Broker, Limited, park(Broker, Unlimited, 50))
        end,
        [
            {[{max_right_balance, 1}], right},
            {[{min_left_balance, -1}], left}
        ]
    ).

% Each bound applies to its own lane only: `left` holds three waiters and
% `right` two, and one more on either is refused
asymmetric_limits_bound_each_lane_apart(_Config) ->
    Broker = cbroker:new([{min_left_balance, -3}, {max_right_balance, 2}]),

    lists:foreach(
        fun({Lane, Room}) ->
            Parked = park(Broker, Lane, Room),
            ?assertMatch({drop, full_lane, _}, cbroker:dynamic_ask(Broker, Lane, offer)),
            drain(Broker, other_lane(Lane), Parked)
        end,
        [{left, 3}, {right, 2}]
    ).

%%

% Without an offer, an ask offers its own pid. The counterpart is parked
% beforehand, so the default timeouts never come into play
default_offer_is_the_asker(Config) ->
    Broker = broker(Config),
    Self = self(),

    lists:foreach(
        fun(AskFun) ->
            {await, Ticket} = cbroker:async_ask(Broker, left),
            ?assertMatch({match, _, Self, _}, AskFun()),
            ?assertMatch({match, _, Self, _}, await(Ticket))
        end,
        [
            fun() -> cbroker:ask(Broker, right) end,
            fun() -> cbroker:ask(Broker, right, Self) end,
            fun() -> cbroker:nb_ask(Broker, right) end,
            fun() -> cbroker:dynamic_ask(Broker, right) end,
            fun() -> cbroker:resumable_ask(Broker, right) end,
            fun() -> cbroker:resumable_ask(Broker, right, Self) end
        ]
    ).

resumable_ask_returns_a_ready_match(Config) ->
    Broker = broker(Config),

    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
    ?assertMatch({match, _, offer_l, _}, cbroker:resumable_ask(Broker, right, offer_r, 5_000)),
    ?assertMatch({match, _, offer_r, _}, await(Ticket)).

resumable_ask_waits_for_a_match(Config) ->
    Broker = broker(Config),

    % Only asks once we're parked, so that we do have to wait
    _ = spawn_link(fun() ->
        wait_until(fun() -> pending_cells(Broker) =/= [] end),
        {match, _, offer_l, _} = cbroker:nb_ask(Broker, right, offer_r)
    end),

    ?assertMatch({match, _, offer_r, _}, cbroker:resumable_ask(Broker, left, offer_l, 5_000)).

% On timeout the ask is still there: it can be matched later, or cancelled
resumable_ask_stays_enqueued_on_timeout(Config) ->
    Broker = broker(Config),

    {timeout, ReplyRef, _Ticket} = cbroker:resumable_ask(Broker, left, offer_l, 0),
    ?assertMatch({match, _, offer_l, _}, cbroker:nb_ask(Broker, right, offer_r)),
    ?assertMatch({match, _, offer_r, _}, await(ReplyRef)),

    {timeout, _, Ticket} = cbroker:resumable_ask(Broker, left, offer_l, 0),
    ?assertMatch({cancelled, _}, cbroker:cancel(Ticket)).

%%

child_spec_defaults_to_no_opts(_Config) ->
    RegName = {local, cbroker_spec_test},
    ?assertEqual(cbroker:child_spec(RegName, []), cbroker:child_spec(RegName)).

% Whatever takes a broker takes its name, in any of its forms, and reaches
% the very same broker as the reference the name resolves to
named_broker_is_asked_by_name(_Config) ->
    lists:foreach(
        fun({RegName, Name}) ->
            Server = start_named(RegName, []),
            Broker = cbroker:resolve_name(Name),
            ?assertEqual(Broker, cbroker:resolve_name(RegName)),
            ?assertEqual(cbroker:debug_info(Broker), cbroker:debug_info(Name)),

            % Parked through the reference, matched through the name
            lists:foreach(
                fun(AskByName) ->
                    {await, Ticket} = cbroker:async_ask(Broker, left, offer_l),
                    ?assertMatch({match, _, offer_l, _}, AskByName()),
                    ?assertMatch({match, _, offer_r, _}, await(Ticket))
                end,
                [
                    fun() -> cbroker:ask(Name, right, offer_r, 5_000) end,
                    fun() -> cbroker:nb_ask(Name, right, offer_r) end,
                    fun() -> cbroker:dynamic_ask(Name, right, offer_r, ticket) end,
                    fun() -> cbroker:resumable_ask(Name, right, offer_r, 5_000) end
                ]
            ),

            % And the other way around
            {await, ByName} = cbroker:async_ask(Name, left, offer_l, ticket),
            ?assertMatch({match, _, offer_l, _}, cbroker:nb_ask(Broker, right, offer_r)),
            ?assertMatch({match, _, offer_r, _}, await(ByName)),

            ok = gen_server:stop(Server)
        end,
        [
            {{local, cbroker_named_test}, cbroker_named_test},
            {{global, {?MODULE, global}}, {global, {?MODULE, global}}},
            {{via, global, {?MODULE, via}}, {via, global, {?MODULE, via}}}
        ]
    ).

% Options reach the broker, except that it always depends on its server
named_broker_takes_opts(_Config) ->
    Name = cbroker_opts_test,
    Server = start_named({local, Name}, [{depends_on_creator, false}, {max_queue_len, 3}]),

    ?assertEqual(true, opt(depends_on_creator, Name)),
    ?assertEqual({-3, 3}, {opt(min_left_balance, Name), opt(max_right_balance, Name)}),

    ok = gen_server:stop(Server).

% A server that stops for a healthy reason closes its broker and gives up
% the name
stopping_closes_and_forgets_the_name(_Config) ->
    Name = cbroker_stopping_test,

    lists:foreach(
        fun(Reason) ->
            Server = start_named({local, Name}, []),
            {await, Ticket} = cbroker:async_ask(Name, left, offer_l),

            ok = gen_server:stop(Server, Reason, 5_000),

            ?assertMatch({drop, closed, _}, await(Ticket)),
            ?assertError({broker_not_found, Name}, cbroker:resolve_name(Name)),
            ?assertError({broker_not_found, Name}, cbroker:nb_ask(Name, right, offer_r))
        end,
        [normal, shutdown, {shutdown, restarting}]
    ).

% Closing by name leaves the server up and the name in place, so that asks
% fail as `closed` until the server is restarted
closing_by_name_keeps_the_name(_Config) ->
    Name = cbroker_closing_test,
    Server = start_named({local, Name}, []),
    Broker = cbroker:resolve_name(Name),
    {await, Ticket} = cbroker:async_ask(Name, left, offer_l),

    ?assertEqual(ok, cbroker:close(Name)),

    ?assertMatch({drop, closed, _}, await(Ticket)),
    ?assert(is_process_alive(Server)),
    ?assertEqual(Broker, cbroker:resolve_name(Name)),
    ?assertError(closed, cbroker:nb_ask(Name, right, offer_r)),

    ok = gen_server:stop(Server),
    ?assertError({broker_not_found, Name}, cbroker:close(Name)).

% After a crash the name still points at the closed broker, so that asks fail
% as `closed` until a restarted server replaces it
crashing_keeps_the_name_until_restarted(_Config) ->
    Name = cbroker_crashing_test,
    Server = start_named({local, Name}, []),
    Broker = cbroker:resolve_name(Name),
    {await, Ticket} = cbroker:async_ask(Name, left, offer_l),

    ok = gen_server:stop(Server, crashed, 5_000),

    ?assertMatch({drop, closed, _}, await(Ticket)),
    ?assertEqual(Broker, cbroker:resolve_name(Name)),
    ?assertError(closed, cbroker:nb_ask(Name, right, offer_r)),

    Restarted = start_named({local, Name}, []),
    ?assertNotEqual(Broker, cbroker:resolve_name(Name)),
    ?assertMatch({drop, match_not_found, _}, cbroker:nb_ask(Name, right, offer_r)),

    ok = gen_server:stop(Restarted).

unknown_and_invalid_names_are_rejected(_Config) ->
    Unknown = cbroker_unknown_test,
    ?assertError({broker_not_found, Unknown}, cbroker:resolve_name(Unknown)),
    ?assertError({broker_not_found, Unknown}, cbroker:nb_ask(Unknown, left, offer_l)),
    ?assertError({broker_not_found, Unknown}, cbroker:debug_info(Unknown)),

    lists:foreach(
        fun(Name) -> ?assertError({invalid_name, Name}, cbroker:resolve_name(Name)) end,
        ["name", {local, "name"}, {via, "module", name}, {elsewhere, name}]
    ).

% Unknown calls and casts are ignored, and a code change keeps the state as
% long as it recognizes it
named_server_survives_what_it_does_not_handle(_Config) ->
    Name = cbroker_unhandled_test,
    Server = start_named({local, Name}, []),

    ?assertExit({timeout, _}, gen_server:call(Server, unknown_call, 100)),
    ok = gen_server:cast(Server, unknown_cast),

    ok = sys:suspend(Server),
    ?assertEqual(ok, sys:change_code(Server, cbroker_persistent, undefined, [])),
    Unknown = sys:replace_state(Server, fun(Known) -> {unknown, Known} end),
    % `sys` wraps whatever isn't `{ok, _}` in an error of its own
    ?assertEqual(
        {error, {error, {cannot_convert_state, Unknown}}},
        sys:change_code(Server, cbroker_persistent, undefined, [])
    ),
    _ = sys:replace_state(Server, fun({unknown, Known}) -> Known end),
    ok = sys:resume(Server),

    ?assertMatch({drop, match_not_found, _}, cbroker:nb_ask(Name, left, offer_l)),
    ok = gen_server:stop(Server).

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

% Parked `right` asks minus parked `left` ones, plus any asks in flight
queue_balance(Broker) ->
    {queue_balance, Balance} = lists:keyfind(queue_balance, 1, debug_value(stats, Broker)),
    Balance.

% Parks Amount asks on Lane, asserting each one did park rather than drop
park(Broker, Lane, Amount) ->
    Before = queue_balance(Broker),
    Tickets = [
        Ticket
     || N <- lists:seq(1, Amount), {await, Ticket} <- [cbroker:async_ask(Broker, Lane, {Lane, N})]
    ],
    ?assertEqual(Before + Amount * lane_weight(Lane), queue_balance(Broker)),
    Tickets.

% Parks an ask from a process of its own, which can then be killed
park_in_process(Broker, Lane) ->
    Parent = self(),
    Pid = spawn(fun() ->
        {await, _} = cbroker:async_ask(Broker, Lane, parked),
        Parent ! {self(), parked},
        receive
            never -> ok
        end
    end),
    receive
        {Pid, parked} -> Pid
    after 5_000 ->
        ct:fail({not_parked, Pid})
    end.

% Matches every parked ticket from Lane, which must leave the broker balanced
drain(Broker, Lane, Tickets) ->
    lists:foreach(fun(_) -> {match, _, _, _} = cbroker:nb_ask(Broker, Lane, drain) end, Tickets),
    lists:foreach(fun(T) -> ?assertMatch({match, _, drain, _}, await(T)) end, Tickets),
    ?assertEqual(0, queue_balance(Broker)).

lane_weight(left) -> -1;
lane_weight(right) -> +1.

other_lane(left) -> right;
other_lane(right) -> left.

reply_ref_arg(ticket) -> ticket;
reply_ref_arg(reply_ref) -> make_ref().

% What a reply is tagged with: the ticket, unless a reference was given
expected_tag(ticket, Ticket) -> Ticket;
expected_tag(ReplyRef, _Ticket) -> ReplyRef.

% The oldest message in the mailbox, whatever it is
next_message() ->
    receive
        Msg -> Msg
    after 5_000 ->
        ct:fail(no_message)
    end.

% For what happens asynchronously, such as the DOWN of a killed waiter
wait_until(Fun) ->
    wait_until(Fun, 500).

wait_until(_Fun, 0) ->
    ct:fail(condition_never_met);
wait_until(Fun, TriesLeft) ->
    case Fun() of
        true ->
            ok;
        %
        false ->
            timer:sleep(10),
            wait_until(Fun, TriesLeft - 1)
    end.

% One count per scheduler; which one is which depends on which asked first
local_pool_counts(Pool, Broker) ->
    [
        Count
     || LocalState <- debug_value(local_states, Broker), {P, Count} <- LocalState, P =:= Pool
    ].

% Starts a named broker's server from its child spec, as a supervisor would.
% Unlinked, so that stopping it for any reason doesn't take the test case along
start_named(RegName, Opts) ->
    #{start := {Module, Function, Args}} = cbroker:child_spec(RegName, Opts),
    {ok, Server} = apply(Module, Function, Args),
    true = unlink(Server),
    Server.

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
