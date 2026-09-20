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

-module(cbroker_bench).

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
%% Type Definitions
%% ------------------------------------------------------------------

-record(proc_stats, {samples}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

bench1(Impl, OfferName, TotalIterations, TotalPidsAmount) ->
    LeftFun = left_fun(Impl),
    RightFun = right_fun(Impl),

    Offer = generate_exchange_value(OfferName),

    true = (TotalPidsAmount >= 2),
    LeftPidsAmount = TotalPidsAmount div 2,
    RightPidsAmount = TotalPidsAmount - LeftPidsAmount,

    LeftIterationsList = iterations_list(TotalIterations, LeftPidsAmount),
    RightIterationsList = iterations_list(TotalIterations, RightPidsAmount),

    LeftPids = launch_processes(LeftFun, Offer, LeftIterationsList),
    RightPids = launch_processes(RightFun, Offer, RightIterationsList),

    Pids = pids_join(LeftPids, RightPids),
    PidSet = maps:from_keys(Pids, v),

    lists:foreach(fun erlang:garbage_collect/1, processes()),
    erlang:garbage_collect(),
    %logger:notice("Starting..."),
    StartTs = erlang:monotonic_time(),
    processes_send(Pids, go),
    receive_done(PidSet),
    FinishTs = erlang:monotonic_time(),

    logger:debug("Collecting stats!"),
    timer:sleep(100),

    processes_send(Pids, stats),
    Samples = receive_results(PidSet),

    TotalSamples = length(Samples),
    GroupedSamples = maps:groups_from_list(fun sample_group/1, Samples),

    SortedGroups = lists:keysort(
        1, lists:map(fun group_with_sorting_key/1, maps:to_list(GroupedSamples))
    ),

    TotalDurationSecs =
        round(
            (FinishTs - StartTs) /
                erlang:convert_time_unit(1, millisecond, native)
        ) / 1000,

    [
        {total_duration_secs, TotalDurationSecs},
        {delays_per_group,
            lists:map(fun(Group) -> group_stats(Group, TotalSamples) end, SortedGroups)}
    ].

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

iterations_list(TotalIterations, PidsAmount) ->
    Base = TotalIterations div PidsAmount,
    Rem = TotalIterations rem PidsAmount,
    true = Base >= 1,
    [
        Base +
            (if
                I =< Rem -> 1;
                true -> 0
            end)
     || I <- lists:seq(1, PidsAmount)
    ].

sample_group({blocked, _}) ->
    blocked;
sample_group({instant, _, _}) ->
    instant;
sample_group({instant, _}) ->
    instant;
sample_group({retried, RetryCount, _}) ->
    {retried, RetryCount}.

group_with_sorting_key({GroupKey, _} = Pair) ->
    {group_sorting_key(GroupKey), Pair}.

group_sorting_key(instant) ->
    [1];
group_sorting_key({retried, RetryCount}) ->
    [2, RetryCount];
group_sorting_key(blocked) ->
    [3].

group_stats({_SortingKey, {GroupKey, Samples}}, TotalSamples) ->
    Delays = lists:map(fun last_element/1, Samples),
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

last_element(Tuple) ->
    Size = tuple_size(Tuple),
    element(Size, Tuple).

percentile_us(Percentile, Bag) ->
    {value, Value} = xb5_bag:percentile(Percentile, Bag),
    native_to_us(Value).

%    DelaysBag = xb5_bag:from_list(Delays),
%    RpsList = rps_list(StartTss),
%    RpsBag = xb5_bag:from_list(RpsList),

%    [
%     {rps, [
%        {average, floor(lists:sum(RpsList) / length(RpsList))},
%        {percentiles, [
%           {median, floor(element(2, xb5_bag:percentile(0.50, RpsBag)))},
%           {p95, floor(element(2, xb5_bag:percentile(0.95, RpsBag)))},
%           {p99, floor(element(2, xb5_bag:percentile(0.99, RpsBag)))}
%        ]}
%     ]},
%     {delays, [
%        {average, native_to_us(lists:sum(Delays) / length(Delays))},
%        {percentiles, [
%           {median, native_to_us(xb5_bag:percentile(0.50, DelaysBag))},
%           {p95, native_to_us(xb5_bag:percentile(0.95, DelaysBag))},
%           {p99, native_to_us(xb5_bag:percentile(0.99, DelaysBag))}
%        ]}
%     ]}
%    ].

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

%rps_list(StartTss) ->
%    Bag = xb5_bag:from_list(StartTss),
%    Sorted = lists:usort(xb5_bag:to_list(Bag)),
%    rps_list_recur(Sorted, Bag).
%
%rps_list_recur([WindowEnd | Next], Bag) ->
%    WindowStart = WindowEnd - erlang:convert_time_unit(1, second, native),
%
%    case xb5_bag:larger(WindowStart, Bag) of
%        {found, StartTs} ->
%            case WindowEnd - StartTs of
%                0 ->
%                    rps_list_recur(Next, Bag);
%                %
%                Duration ->
%                    {rank, StartRank} = xb5_bag:rank(StartTs, Bag),
%                    {rank, EndRank} = xb5_bag:rank(WindowEnd, Bag),
%                    InstantRps = (EndRank - StartRank + 1) / (Duration / erlang:convert_time_unit(1, second, native)),
%                    [InstantRps | rps_list_recur(Next, Bag)]
%            end;
%        %
%        none ->
%            rps_list_recur(Next, Bag)
%    end;
%rps_list_recur([], _) ->
%    [].

%%

left_fun(simple) ->
    fun simple_left_iteration/1;
left_fun(cbroker) ->
    fun cbroker_left_iteration/1.

right_fun(simple) ->
    fun simple_right_iteration/1;
right_fun(cbroker) ->
    fun cbroker_right_iteration/1.

%%

simple_left_iteration(Offer) ->
    simple_iteration(left, Offer).

simple_right_iteration(Offer) ->
    simple_iteration(right, Offer).

simple_iteration(Side, Offer) ->
    StartTs = erlang:monotonic_time(),
    {await, Pid, Tag} = cbroker_simple:async_ask(Side, self(), Offer),

    receive
        {Ref, Reply} when Ref =:= Tag ->
            FinalTs = erlang:monotonic_time(),
            {match, _MatchRef, _, _} = Reply,
            {blocked, FinalTs - StartTs};
        %
        {'DOWN', Ref, _, _, Reason} when Ref =:= Tag ->
            exit({queue_down, Pid, Reason})
    end.

%%

cbroker_left_iteration(Offer) ->
    cbroker_iteration(left, Offer).

cbroker_right_iteration(Offer) ->
    cbroker_iteration(right, Offer).

cbroker_iteration(Side, Offer) ->
    StartTs = erlang:monotonic_time(),

    Broker = cbroker:resolve_name(test),
    cbroker_iteration_recur(StartTs, Broker, Side, Offer, 0).

cbroker_iteration_recur(StartTs, Broker, Side, Offer, RetryCount) ->
    case cbroker:dynamic_ask(Broker, Side, Offer) of
        {await, Tag} ->
            cbroker_iteration_await(StartTs, Tag);
        %
        {match, _, _, Sojourn} ->
            FinalTs = erlang:monotonic_time(),

            case RetryCount of
                0 ->
                    % FIXME
                    {instant, FinalTs - StartTs, Sojourn};
                _ ->
                    {retried, RetryCount, FinalTs - StartTs}
            end;
        %
        retry ->
            cbroker_iteration_recur(StartTs, Broker, Side, Offer, RetryCount + 1)
    end.

cbroker_iteration_await(StartTs, Tag) ->
    receive
        {T, Result} when T =:= Tag ->
            FinalTs = erlang:monotonic_time(),
            {match, _, _, _} = Result,
            {blocked, FinalTs - StartTs}
    end.

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
            Samples ++ receive_results(RemainingPidSet)
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

launch_processes(RunFun, Offer, [Iterations | Next]) ->
    Parent = self(),
    [
        spawn_link(fun() -> start_process(Parent, RunFun, Offer, Iterations) end)
        | launch_processes(RunFun, Offer, Next)
    ];
launch_processes(_, _, []) ->
    [].

start_process(Parent, RunFun, Offer, Iterations) ->
    receive
        go ->
            erlang:yield(),
            Samples = run_process(RunFun, Offer, Iterations),
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

run_process(RunFun, Offer, Iterations) when Iterations > 0 ->
    Timestamps = RunFun(Offer),
    [Timestamps | run_process(RunFun, Offer, Iterations - 1)];
run_process(_, _, 0) ->
    [].
