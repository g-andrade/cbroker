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

-module(cbroker_quickbench).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of the `cbroker` public API.".
-endif.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    bench1/4
]).

-ignore_xref([
    bench1/4
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

%-define(SAMPLING_MASK, 16#F).
%-define(SAMPLING_MULTIPLIER, (1 + ?SAMPLING_MASK)).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

-record(proc_stats, {samples}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

bench1(Impl, OfferName, Timeout, TotalPidsAmount) when is_atom(OfferName) ->
    Offer = generate_exchange_value(OfferName),
    bench1(Impl, Offer, Timeout, TotalPidsAmount);
bench1(Impl, Offer, Timeout, TotalPidsAmount) ->
    Target = setup(Impl),
    try
        run_bench1(Impl, Target, Offer, Timeout, TotalPidsAmount)
    after
        teardown(Impl, Target)
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

run_bench1(Impl, Target, Offer, Timeout, TotalPidsAmount) ->
    LeftFun = left_fun(Impl, Target),
    RightFun = right_fun(Impl, Target),

    true = (TotalPidsAmount >= 2),
    LeftPidsAmount = TotalPidsAmount div 2,
    RightPidsAmount = TotalPidsAmount - LeftPidsAmount,

    LeftPids = launch_processes(LeftFun, Offer, LeftPidsAmount),
    RightPids = launch_processes(RightFun, Offer, RightPidsAmount),

    Pids = pids_join(LeftPids, RightPids),
    PidSet = maps:from_keys(Pids, v),

    lists:foreach(fun erlang:garbage_collect/1, processes()),
    erlang:garbage_collect(),
    %logger:notice("Starting..."),
    StartTs = erlang:monotonic_time(),
    ScheduledFinishTsMillis = erlang:convert_time_unit(StartTs, native, millisecond) + Timeout,
    TimerRef = erlang:send_after(ScheduledFinishTsMillis, self(), timeout, [{abs, true}]),
    processes_send(Pids, go),

    receive
        timeout ->
            false = erlang:cancel_timer(TimerRef),
            processes_send(Pids, stop_asking),
            teardown(Impl, Target)
    end,

    receive_done(PidSet),

    logger:debug("Collecting stats!"),
    %timer:sleep(100),

    processes_send(Pids, stats),
    Samples = receive_results(PidSet),

    {OldestSampleStartTs, NewestSampleFinishTs} = total_samples_duration_timestamps(Samples),

    TotalSamples = length(Samples),
    GroupedSamples = maps:groups_from_list(fun sample_group/1, Samples),

    SortedGroups = lists:keysort(
        1, lists:map(fun group_with_sorting_key/1, maps:to_list(GroupedSamples))
    ),

    TotalDurationSecs =
        round(
            (NewestSampleFinishTs - OldestSampleStartTs) /
                erlang:convert_time_unit(1, millisecond, native)
        ) / 1000,

    [
        {total_duration_secs, TotalDurationSecs},
        {total_requests, length(Samples)},
        {requests_per_second, rps_stats(Samples)},
        {delays_per_group,
            lists:map(fun(Group) -> group_stats(Group, TotalSamples) end, SortedGroups)}
    ].

total_samples_duration_timestamps([{_SampleType, StartTs, FinishTs} | Next]) ->
    total_samples_duration_timestamps_recur(Next, StartTs, FinishTs).

total_samples_duration_timestamps_recur(
    [{_SampleType, StartTs, FinishTs} | Next], OldestStartTs, NewestFinishTs
) ->
    total_samples_duration_timestamps_recur(
        Next, min(OldestStartTs, StartTs), max(NewestFinishTs, FinishTs)
    );
total_samples_duration_timestamps_recur([], OldestStartTs, NewestFinishTs) ->
    {OldestStartTs, NewestFinishTs}.

sample_group({blocked, _, _}) ->
    blocked;
sample_group({{overloaded, _}, _, _}) ->
    % This simplifies analysis but it's something to keep an eye on
    blocked;
sample_group({instant, _, _}) ->
    instant.

group_with_sorting_key({GroupKey, _} = Pair) ->
    {group_sorting_key(GroupKey), Pair}.

group_sorting_key(instant) ->
    [1];
group_sorting_key({overloaded, OverloadCount}) ->
    [2, OverloadCount];
group_sorting_key(blocked) ->
    [3].

group_stats({_SortingKey, {GroupKey, Samples}}, TotalSamples) ->
    Delays = [sample_delay(Sample) || Sample <- Samples],
    Count = length(Delays),
    PercentageOfTotal = round(1000 * Count / TotalSamples) / 10,

    Bag = xb5_bag:from_list(Delays),

    Stats = [
        {percentage, PercentageOfTotal},
        {average, native_to_us(lists:sum(Delays) / Count)},
        {percentiles, [
            {median, percentile_us(0.50, Bag)},
            {p95, percentile_us(0.95, Bag)},
            {p99, percentile_us(0.99, Bag)}
        ]}
    ],

    {GroupKey, Stats}.

sample_delay({_Type, StartTs, EndTs}) ->
    EndTs - StartTs.

percentile_us(Percentile, Bag) ->
    {value, Value} = xb5_bag:percentile(Percentile, Bag),
    native_to_us(Value).

generate_exchange_value(smallest) ->
    self();
generate_exchange_value(tuple128) ->
    L = lists:seq(1, 128),
    list_to_tuple(L);
generate_exchange_value(tuple1024) ->
    L = lists:seq(1, 1024),
    list_to_tuple(L);
generate_exchange_value(tuple10_000) ->
    L = lists:seq(1, 10_000),
    list_to_tuple(L).

native_to_us({value, Value}) ->
    native_to_us(Value);
native_to_us(Interval) when is_number(Interval) ->
    round(Interval / erlang:convert_time_unit(1, microsecond, native)).

%%

rps_stats(Stats) ->
    SortedStats = lists:keysort(3, Stats),
    Window = queue:new(),
    WindowSize = 0,
    MinPeriod = erlang:convert_time_unit(100, millisecond, native),
    OneSecond = erlang:convert_time_unit(1, second, native),

    case rps_stats_recur(SortedStats, Window, WindowSize, MinPeriod, OneSecond) of
        [] ->
            not_available;
        %
        RpsValues ->
            RpsBag = xb5_bag:from_list(RpsValues),
            Avg = lists:sum(RpsValues) / xb5_bag:size(RpsBag),

            [
                {average, round_rps(Avg)},
                {percentiles, [
                    {p01, round_rps(xb5_bag:percentile(0.01, RpsBag))},
                    {p05, round_rps(xb5_bag:percentile(0.05, RpsBag))},
                    {median, round_rps(xb5_bag:percentile(0.50, RpsBag))},
                    {p95, round_rps(xb5_bag:percentile(0.95, RpsBag))},
                    {p99, round_rps(xb5_bag:percentile(0.99, RpsBag))}
                ]}
            ]
    end.

round_rps({value, Value}) ->
    round_rps(Value);
round_rps(Value) ->
    floor(Value).

rps_stats_recur(
    [{_SampleType, _StartTs, FinishTs} | Next], Window, WindowSize, MinPeriod, OneSecond
) ->
    % we gotta pick one, but excessive delays may distort the results
    SampleTs = FinishTs,
    [Window2 | WindowSize2] = rps_stats_drop(SampleTs, Window, WindowSize, OneSecond),
    Window3 = queue:in(SampleTs, Window2),
    WindowSize3 = WindowSize2 + 1,

    case maybe_rps_value(Window3, WindowSize3, MinPeriod, OneSecond) of
        no ->
            rps_stats_recur(Next, Window3, WindowSize3, MinPeriod, OneSecond);
        %
        Rps ->
            [Rps | rps_stats_recur(Next, Window3, WindowSize3, MinPeriod, OneSecond)]
    end;
rps_stats_recur([], _, _, _, _) ->
    [].

rps_stats_drop(SampleTs, Window, WindowSize, OneSecond) ->
    case queue:is_empty(Window) orelse [head | queue:head(Window)] of
        true ->
            [Window | WindowSize];
        %
        [head | OldestTs] ->
            case SampleTs - OldestTs of
                Diff when Diff >= OneSecond ->
                    Window2 = queue:drop(Window),
                    [Window2 | WindowSize - 1];
                %
                Diff when Diff >= 0 ->
                    [Window | WindowSize]
            end
    end.

maybe_rps_value(Window, WindowSize, MinPeriod, OneSecond) ->
    Oldest = queue:head(Window),
    Newest = queue:last(Window),

    case Newest - Oldest of
        Diff when Diff >= MinPeriod ->
            WindowSize * (OneSecond / Diff);
        %
        Diff when Diff >= 0 ->
            no
    end.

%%

% The bench owns what it measures: a fresh broker, or the `cbroker_simple`
% baseline server
setup(simple) ->
    {ok, Pid} = cbroker_simple:start_link(on_heap),
    Pid;
setup(simple_off_heap) ->
    {ok, Pid} = cbroker_simple:start_link(off_heap),
    Pid;
setup(cbroker) ->
    {ok, Pid} = cbroker_persistent:start_link({local, ?MODULE}, []),
    BrokerRef = cbroker:resolve_name(?MODULE),
    [Pid | BrokerRef].

teardown(simple, Pid) ->
    ok = sys_terminate_or_noproc(Pid);
teardown(simple_off_heap, Pid) ->
    ok = sys_terminate_or_noproc(Pid);
teardown(cbroker, [Pid | _BrokerRef]) ->
    ok = sys_terminate_or_noproc(Pid).

sys_terminate_or_noproc(Pid) ->
    try
        sys:terminate(Pid, normal)
    catch
        exit:{Reason, {sys, terminate, [Pid, normal]}} when Reason =:= noproc; Reason =:= normal ->
            ok
    end.

left_fun(Implementation, Pid) when Implementation =:= simple; Implementation =:= simple_off_heap ->
    fun(Offer, AskCounter, Acc) -> simple_iteration(Pid, left, Offer, AskCounter, Acc) end;
left_fun(cbroker, Broker) ->
    fun(Offer, AskCounter, Acc) -> cbroker_iteration(Broker, left, Offer, AskCounter, Acc) end.

right_fun(Implementation, Pid) when Implementation =:= simple; Implementation =:= simple_off_heap ->
    fun(Offer, AskCounter, Acc) -> simple_iteration(Pid, right, Offer, AskCounter, Acc) end;
right_fun(cbroker, Broker) ->
    fun(Offer, AskCounter, Acc) -> cbroker_iteration(Broker, right, Offer, AskCounter, Acc) end.

%%

simple_iteration(Pid, Side, Offer, _AskCounter, Acc) ->
    StartTs = erlang:monotonic_time(),

    case cbroker_simple:async_ask(Pid, Side, self(), Offer) of
        {await, Tag} ->
            receive
                {Ref, Reply} when Ref =:= Tag ->
                    demonitor(Ref),
                    FinalTs = erlang:monotonic_time(),
                    {match, _MatchRef, _, _} = Reply,
                    [{blocked, StartTs, FinalTs} | Acc];
                %
                {'DOWN', Ref, _, _, Reason} when Ref =:= Tag ->
                    receive
                        stop_asking ->
                            throw(finished_asking)
                    after 0 ->
                        exit({queue_down, Pid, Reason})
                    end
            end;
        %
        stopped ->
            throw(finished_asking)
    end.

%%

cbroker_iteration([_Pid | BrokerRef], Side, Offer, AskCounter, Acc) ->
    StartTs = erlang:monotonic_time(),
    cbroker_iteration_recur(BrokerRef, Side, Offer, AskCounter, StartTs, Acc, 0).

cbroker_iteration_recur(BrokerRef, Side, Offer, AskCounter, StartTs, Acc, OverloadCount) ->
    try cbroker:dynamic_ask(BrokerRef, Side, Offer) of
        {await, Tag} ->
            cbroker_iteration_await(StartTs, Tag, AskCounter, Acc);
        %
        {match, _, _, _} ->
            FinalTs = erlang:monotonic_time(),

            case OverloadCount > 0 of
                true ->
                    [{{overloaded, OverloadCount}, StartTs, FinalTs} | Acc];
                _ ->
                    [{instant, StartTs, FinalTs} | Acc]
            end;
        %
        {drop, broker_overloaded, _} ->
            cbroker_iteration_recur(
                BrokerRef, Side, Offer, AskCounter, StartTs, Acc, OverloadCount + 1
            );
        %
        {drop, broker_closed, _} ->
            throw(finished_asking)
    catch
        error:broker_closed ->
            throw(finished_asking)
    end.

cbroker_iteration_await(StartTs, Tag, _AskCounter, Acc) ->
    receive
        Msg ->
            case Msg of
                {T, Result} when T =:= Tag ->
                    FinalTs = erlang:monotonic_time(),
                    {match, _, _, _} = Result,
                    [{blocked, StartTs, FinalTs} | Acc];
                %
                stop_asking ->
                    throw(finished_asking)
            end
    end.

%should_sample(_AskCounter) ->
%    true.
%    %(AskCounter band ?SAMPLING_MASK) =:= 0.

%%

receive_done(PidSet) when map_size(PidSet) > 0 ->
    receive
        {done, Pid} when is_map_key(Pid, PidSet) ->
            RemainingPidSet = maps:remove(Pid, PidSet),
            receive_done(RemainingPidSet)
    end;
receive_done(#{}) ->
    ok.

receive_results(PidSet) when map_size(PidSet) > 0 ->
    receive
        {finished, Pid, Stats} when is_map_key(Pid, PidSet) ->
            RemainingPidSet = maps:remove(Pid, PidSet),
            #proc_stats{samples = Samples} = Stats,
            lists:reverse(Samples, receive_results(RemainingPidSet))
    end;
receive_results(#{}) ->
    [].

pids_join([LeftPid | NextLeft], [RightPid | NextRight]) ->
    [LeftPid, RightPid | pids_join(NextLeft, NextRight)];
pids_join([], Right) ->
    Right;
pids_join(Left, []) ->
    Left.

processes_send([Pid | Next], Msg) ->
    Pid ! Msg,
    processes_send(Next, Msg);
processes_send([], _) ->
    ok.

launch_processes(RunFun, Offer, Amount) when Amount > 0 ->
    Parent = self(),
    [
        spawn_link(fun() -> start_process(Parent, RunFun, Offer) end)
        | launch_processes(RunFun, Offer, Amount - 1)
    ];
launch_processes(_, _, 0) ->
    [].

start_process(Parent, RunFun, Offer) ->
    receive
        go ->
            erlang:yield(),
            Samples = run_process(RunFun, Offer, 0, []),
            _ = Parent ! {done, self()},

            receive
                stats ->
                    Stats = #proc_stats{
                        samples = Samples
                    },
                    _ = Parent ! {finished, self(), Stats},
                    exit(normal)
            end
    end.

run_process(RunFun, Offer, AskCounter, Acc) ->
    try RunFun(Offer, AskCounter, Acc) of
        UpdatedAcc ->
            run_process(RunFun, Offer, AskCounter + 1, UpdatedAcc)
    catch
        finished_asking ->
            Acc
    end.
