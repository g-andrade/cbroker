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
    start_timestamps :: [integer()],
    delays :: [number()]
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
    processes_go(LeftPids, RightPids),
    StatsList = receive_results(PidSet),

    Delays = lists:flatmap(fun (#proc_stats{delays = Delays}) -> Delays end, StatsList),
    StartTss = lists:flatmap(fun (#proc_stats{start_timestamps = List}) -> List end, StatsList),

    DelaysBag = xb5_bag:from_list(Delays),
    RpsList = rps_list(StartTss),
    RpsBag = xb5_bag:from_list(RpsList),

    [
     {rps, [
        {average, floor(lists:sum(RpsList) / length(RpsList))},
        {percentiles, [
           {median, floor(element(2, xb5_bag:percentile(0.50, RpsBag)))},
           {p95, floor(element(2, xb5_bag:percentile(0.95, RpsBag)))},
           {p99, floor(element(2, xb5_bag:percentile(0.99, RpsBag)))}
        ]}
     ]},
     {delays, [
        {average, native_to_us(lists:sum(Delays) / length(Delays))},
        {percentiles, [
           {median, native_to_us(xb5_bag:percentile(0.50, DelaysBag))},
           {p95, native_to_us(xb5_bag:percentile(0.95, DelaysBag))},
           {p99, native_to_us(xb5_bag:percentile(0.99, DelaysBag))}
        ]}
     ]}
    ].

native_to_us({value, Value}) ->
    native_to_us(Value);
native_to_us(Interval) when is_number(Interval) ->
    round(Interval / erlang:convert_time_unit(1, microsecond, native)).

samples_to_delays([Ts1 | [Ts2 | _] = Next]) ->
    Duration = Ts2 - Ts1,
    [Duration | samples_to_delays(Next)];
samples_to_delays([_FinalTs]) ->
    [].

%%

rps_list(StartTss) ->
    Bag = xb5_bag:from_list(StartTss),
    Sorted = lists:usort(xb5_bag:to_list(Bag)),
    rps_list_recur(Sorted, Bag).

rps_list_recur([WindowEnd | Next], Bag) ->
    WindowStart = WindowEnd - erlang:convert_time_unit(1, second, native),

    case xb5_bag:larger(WindowStart, Bag) of
        {found, StartTs} ->
            case WindowEnd - StartTs of
                0 ->
                    rps_list_recur(Next, Bag);
                %
                Duration ->
                    {rank, StartRank} = xb5_bag:rank(StartTs, Bag),
                    {rank, EndRank} = xb5_bag:rank(WindowEnd, Bag),
                    InstantRps = (EndRank - StartRank + 1) / (Duration / erlang:convert_time_unit(1, second, native)),
                    [InstantRps | rps_list_recur(Next, Bag)]
            end;
        %
        none ->
            rps_list_recur(Next, Bag)
    end;
rps_list_recur([], _) ->
    [].

%%

left_fun(simple) ->
    fun simple_left_iteration/0;
left_fun(cbroker) ->
    fun cbroker_left_iteration/0.

right_fun(simple) ->
    fun simple_right_iteration/0;
right_fun(cbroker) ->
    fun cbroker_right_iteration/0.

%%

simple_left_iteration() ->
    simple_iteration(left).

simple_right_iteration() ->
    simple_iteration(right).

simple_iteration(Side) ->
    {await, Pid, Tag} = cbroker_simple:async_ask(Side, self(), self()),

    receive
        {Ref, Reply} when Ref =:= Tag ->
            {match, _MatchRef, _} = Reply,
            ok;
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
    {match, _} = ask_side(test, Side, self(), infinity),
    ok.

%%

receive_results(PidSet) when map_size(PidSet) > 0 ->
    receive
        {finished, Pid, Samples} when is_map_key(Pid, PidSet) ->
            RemainingPidSet = maps:remove(Pid, PidSet),
            [Samples | receive_results(RemainingPidSet)]
    end;
receive_results(#{}) ->
    [].

processes_go([Pid1 | Next1], [Pid2 | Next2]) ->
    Pid1 ! go,
    Pid2 ! go,
    processes_go(Next1, Next2);
processes_go([], []) ->
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

            Stats = #proc_stats{
                start_timestamps = lists:sublist(Samples, length(Samples) - 1),
                delays = samples_to_delays(Samples)
            },
            _ = Parent ! {finished, self(), Stats},
            exit(normal)
    end.

run_process(RunFun, Iterations) when Iterations > 0 ->
    StartTs = erlang:monotonic_time(),
    RunFun(),
    [StartTs | run_process(RunFun, Iterations - 1)];
run_process(_, 0) ->
    EndTs = erlang:monotonic_time(),
    [EndTs].


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
