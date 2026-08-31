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

-module(cbroker).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of the `cbroker` public API.".
-endif.

-include("src/cbroker_shared_state.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    ask/1,
    ask/2,
    ask/3,
    ask_r/1,
    ask_r/2,
    ask_r/3,
    %
    async_ask/1,
    async_ask/2,
    async_ask_r/1,
    async_ask_r/2,
    %
    to_list/1,
    %
    bench1/3
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(DEFAULT_TIMEOUT, 5_000).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

-record(proc_stats, {
    samples :: [term()]
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

% Documented public API functions follow the pattern below. Doc attributes are
% guarded by `-ifdef(E48)` so the source still compiles on OTP < 27, which lacks
% EEP-48 `-doc`/`-moduledoc`. Hide internals with `-doc false` / `-moduledoc
% false` (NOT `@private`, which ex_doc ignores). A public function that isn't
% called internally needs `-ignore_xref/1` to satisfy the `exports_not_used`
% xref check.
%
%     -export([add/2]).
%     -ignore_xref([add/2]).
%
%     -ifdef(E48).
%     -doc "Adds two integers.".
%     -endif.
%     -spec add(integer(), integer()) -> integer().
%     add(A, B) ->
%         A + B.

ask(Name) ->
    ask(Name, self()).

ask(Name, Value) ->
    ask(Name, Value, ?DEFAULT_TIMEOUT).

ask(Name, Value, Timeout) ->
    ask_side(Name, left, Value, Timeout).

ask_r(Name) ->
    ask_r(Name, self()).

ask_r(Name, Value) ->
    ask_r(Name, Value, ?DEFAULT_TIMEOUT).

ask_r(Name, Value, Timeout) ->
    ask_side(Name, right, Value, Timeout).

%%

async_ask(Name) ->
    async_ask(Name, self()).

async_ask(Name, Value) ->
    async_ask_side(Name, left, Value).

async_ask_r(Name) ->
    async_ask_r(Name, self()).

async_ask_r(Name, Value) ->
    async_ask_side(Name, right, Value).

to_list(Name) ->
    case cbroker_serv:get_shared_state(Name) of
        #shared_state{broker = Broker} ->
            cbroker_nif:to_list(Broker)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%

bench1(Impl, TotalPidsAmount, Iterations) ->
    LeftFun = left_fun(Impl),
    RightFun = right_fun(Impl),

    SidePidsAmount = TotalPidsAmount div 2,

    LeftPids = launch_processes(SidePidsAmount, LeftFun, Iterations),
    RightPids = launch_processes(SidePidsAmount, RightFun, Iterations),

    PidSet = maps:from_keys(LeftPids ++ RightPids, v),

    StartTs = erlang:monotonic_time(),
    processes_send(LeftPids, RightPids, go),
    receive_done(PidSet),
    FinishTs = erlang:monotonic_time(),

    logger:debug("Collecting stats!"),
    timer:sleep(100),

    processes_send(LeftPids, RightPids, stats),
    Samples = receive_results(PidSet),

    TotalSamples = length(Samples),
    GroupedSamples = maps:groups_from_list(fun sample_group/1, Samples),

    SortedGroups = lists:keysort(1, lists:map(fun group_with_sorting_key/1, maps:to_list(GroupedSamples))),

    TotalDurationSecs = round(
      (FinishTs - StartTs) 
      / erlang:convert_time_unit(1, millisecond, native)
    ) / 1000,

    [
        {total_duration_secs, TotalDurationSecs},
        {delays_per_group, lists:map(fun (Group) -> group_stats(Group, TotalSamples) end, SortedGroups)}
    ].



sample_group({blocked, _}) ->
    blocked;
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
    fun simple_left_iteration/0;
left_fun(cbroker) ->
    fun cbroker_left_iteration/0;
left_fun(cbroker2) ->
    fun cbroker2_left_iteration/0.

right_fun(simple) ->
    fun simple_right_iteration/0;
right_fun(cbroker) ->
    fun cbroker_right_iteration/0;
right_fun(cbroker2) ->
    fun cbroker2_right_iteration/0.

%%

simple_left_iteration() ->
    simple_iteration(left).

simple_right_iteration() ->
    simple_iteration(right).

simple_iteration(Side) ->
    StartTs = erlang:monotonic_time(),
    {await, Pid, Tag} = cbroker_simple:async_ask(Side, self(), self()),

    receive
        {Ref, Reply} when Ref =:= Tag ->
            FinalTs = erlang:monotonic_time(),
            {match, _MatchRef, _} = Reply,
            {blocked, FinalTs - StartTs};
        %
        {'DOWN', Ref, _, _, Reason} when Ref =:= Tag ->
            exit({queue_down, Pid, Reason})
    end.

%%

cbroker_left_iteration() ->
    cbroker_iteration(left).

cbroker_right_iteration() ->
    cbroker_iteration(right).

cbroker_iteration(Side) ->
    StartTs = erlang:monotonic_time(),

    case cbroker_serv:get_shared_state(test) of
        #shared_state{broker = Broker} ->
            cbroker_iteration_recur(StartTs, Broker, Side, 0)
    end.

cbroker_iteration_recur(StartTs, Broker, Side, RetryCount) ->
    case cbroker_nif:ask(Broker, Side, self()) of
        {await, Ticket} ->
            cbroker_iteration_await(StartTs, Ticket);
        %
        {match, _} ->
            FinalTs = erlang:monotonic_time(),
            
            case RetryCount of
                0 ->
                    {instant, FinalTs - StartTs};
                _ ->
                    {retried, RetryCount, FinalTs - StartTs}
            end;
        %
        retry ->
            cbroker_iteration_recur(StartTs, Broker, Side, RetryCount + 1)
    end.

cbroker_iteration_await(StartTs, Ticket) ->
    receive
        {T, Result} when T =:= Ticket ->
            FinalTs = erlang:monotonic_time(),
            {match, _} = Result,
            {blocked, FinalTs - StartTs}
    end.
    
%%

cbroker2_left_iteration() ->
    cbroker2_iteration(left).

cbroker2_right_iteration() ->
    cbroker2_iteration(right).

cbroker2_iteration(Side) ->
    StartTs = erlang:monotonic_time(),

    case cbroker_serv:get_shared_state(test) of
        #shared_state{broker2 = Broker} ->
            cbroker2_iteration_recur(StartTs, Broker, Side, 0)
    end.

cbroker2_iteration_recur(StartTs, Broker, Side, RetryCount) ->
    case cbroker_nif2:ask(Broker, Side, self()) of
        {await, Ticket} ->
            cbroker2_iteration_await(StartTs, Ticket);
        %
        {match, _} ->
            FinalTs = erlang:monotonic_time(),
            
            case RetryCount of
                0 ->
                    {instant, FinalTs - StartTs};
                _ ->
                    {retried, RetryCount, FinalTs - StartTs}
            end;
        %
        retry ->
            cbroker2_iteration_recur(StartTs, Broker, Side, RetryCount + 1)
    end.

cbroker2_iteration_await(StartTs, Ticket) ->
    receive
        {T, Result} when T =:= Ticket ->
            FinalTs = erlang:monotonic_time(),
            {match, _} = Result,
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

processes_send([Pid1 | Next1], [Pid2 | Next2], Msg) ->
    Pid1 ! Msg,
    Pid2 ! Msg,
    processes_send(Next1, Next2, Msg);
processes_send([], [], _) ->
    ok.

launch_processes(Amount, RunFun, Iterations) when Amount > 0 ->
    Parent = self(),
    [
     spawn_link(fun () -> start_process(Parent, RunFun, Iterations) end)
     | launch_processes(Amount - 1, RunFun, Iterations)
    ];
launch_processes(0, _, _) ->
    [].

start_process(Parent, RunFun, Iterations) ->
    receive
        go ->
            Samples = run_process(RunFun, Iterations),
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

run_process(RunFun, Iterations) when Iterations > 0 ->
    Timestamps = RunFun(),
    [Timestamps | run_process(RunFun, Iterations - 1)];
run_process(_, 0) ->
    [].


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

ask_side(Name, Side, Value, Timeout) ->
    case cbroker_serv:get_shared_state(Name) of
        #shared_state{broker = Broker} ->
            %
            case cbroker_nif:ask(Broker, Side, Value) of
                {await, Ticket} ->
                    await_after_ask(Broker, Ticket, Timeout);
                %
                {match, _} = Match ->
                    Match;
                %
                retry ->
                    ask_side(Name, Side, Value, Timeout)
            end;
        %
        none ->
            not_running
    end.

await_after_ask(Broker, Ticket, Timeout) ->
    receive
        {T, Result} when T =:= Ticket ->
            Result
    after
        Timeout ->
            case cbroker_nif:cancel(Broker, Ticket) of
                cancelled ->
                    timeout;
                %
                too_late ->
                    receive
                        {T, Result} when T =:= Ticket ->
                            Result
                    end
            end
    end.

async_ask_side(Name, Side, Value) ->
    case cbroker_serv:get_shared_state(Name) of
        #shared_state{broker = Broker} ->
            case cbroker_nif:ask(Broker, Side, Value, true) of
                retry ->
                    async_ask_side(Name, Side, Value);
                %
                Result ->
                    Result
            end;
        %
        none ->
            not_running
    end.
