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
%% processes: matches pair up exactly, timeouts never swallow a match, and
%% requests left behind by dead processes get reclaimed.
%%
%% Sized by environment variables, so the defaults keep `make test` quick while
%% `make stress` (or CI) can run the same cases for much longer:
%%
%%   CBROKER_STRESS_PROCS_PER_SIDE   (default 8)
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
    killed_waiters_are_reclaimed/1
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
                killed_waiters_are_reclaimed
            ]
        }
    ].

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

% Everyone asks with a generous timeout, so every ask must match, and every
% match must be seen by exactly one process per side, with the offers crossed
every_match_is_paired(Config) ->
    Broker = broker(Config),

    Samples = run_workers(Broker, fun(Side, Offer) ->
        cbroker:ask(Broker, Side, Offer, ?ASK_TIMEOUT_MS)
    end),

    ?assertEqual([], [Sample || {_, _, Reply} = Sample <- Samples, not is_match(Reply)]),
    ?assertEqual(2 * procs_per_side() * iterations(), length(Samples)),
    assert_matches_are_paired(Samples).

% Tiny timeouts make every ask race its own cancellation. A cancellation that
% loses the race must still yield the match, so the pairing holds regardless
timeouts_never_lose_a_match(Config) ->
    Broker = broker(Config),

    Samples = run_workers(Broker, fun(Side, Offer) ->
        cbroker:ask(Broker, Side, Offer, rand:uniform(4) - 1)
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
    Amount = procs_per_side(),

    Waiters = [spawn(fun() -> park_forever(Broker, left, {offer, N}) end) || N <- seq(Amount)],
    ok = wait_until(fun() -> length(pending_cells(Broker)) =:= Amount end),

    lists:foreach(fun(Pid) -> exit(Pid, kill) end, Waiters),
    ok = wait_until(fun() -> pending_cells(Broker) =:= [] end),

    % Nothing parked any more, so there is nothing left to match against
    ?assertMatch({drop, match_unavailable, _}, cbroker:nb_ask(Broker, right, late_offer)).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% Half the processes on each side, each running AskFun for every iteration.
% Returns one {Side, Offer, Reply} sample per ask
run_workers(_Broker, AskFun) ->
    Parent = self(),
    Iterations = iterations(),

    Workers = [
        spawn_monitor(fun() -> Parent ! {self(), worker_samples(Side, Iterations, AskFun)} end)
     || Side <- [left, right], _ <- seq(procs_per_side())
    ],

    lists:append([collect_samples(Pid, MonRef) || {Pid, MonRef} <- Workers]).

worker_samples(Side, Iterations, AskFun) ->
    [
        begin
            Offer = {Side, self(), N},
            {Side, Offer, AskFun(Side, Offer)}
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

% Each match ref must show up exactly twice, once per side, and each side must
% have received the other's offer
assert_matches_are_paired(Samples) ->
    ByMatchRef = lists:foldl(
        fun({Side, Offer, {match, MatchRef, CounterOffer, _}}, Acc) ->
            Sample = {Side, Offer, CounterOffer},
            maps:update_with(MatchRef, fun(Pairs) -> [Sample | Pairs] end, [Sample], Acc)
        end,
        #{},
        Samples
    ),

    Unpaired = maps:filter(fun(_, Pairs) -> not is_pair(Pairs) end, ByMatchRef),
    ?assertEqual(#{}, Unpaired).

is_pair([{SideA, OfferA, CounterOfferA}, {SideB, OfferB, CounterOfferB}]) ->
    SideA =/= SideB andalso
        CounterOfferA =:= OfferB andalso
        CounterOfferB =:= OfferA;
is_pair(_Pairs) ->
    false.

park_forever(Broker, Side, Offer) ->
    {await, _} = cbroker:async_ask(Broker, Side, Offer),
    receive
        never -> ok
    end.

%%

is_match({match, _, _, _}) -> true;
is_match(_Reply) -> false.

drop_of_reason({drop, Reason, SojournTime}, Reason) -> {drop, Reason, SojournTime};
drop_of_reason(_Reply, _Reason) -> no_such_drop.

procs_per_side() ->
    env_int("CBROKER_STRESS_PROCS_PER_SIDE", 8).

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
        Msg -> [Msg | flush_mailbox()]
    after 0 -> []
    end.
