-module(cbroker_pool).

-include_lib("stdlib/include/assert.hrl").

-ifdef(E48).
-moduledoc false.
-endif.

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    child_spec/0,
    start_link/0,
    get_broker/0,
    run/2
]).

-ignore_xref([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(SERVER, ?MODULE).

-define(SAMPLE(Timestamp, Balance), {Timestamp, Balance}).

-define(TARGET_INTERVAL, 100).

-define(PROBING_INTERVAL, 100).

%% ------------------------------------------------------------------
%% Record and Type Definitions
%% ------------------------------------------------------------------

-record(state, {
    settings :: settings(),
    broker :: reference(),
    workers :: #{pid() => worker()},
    retirees :: gb_sets:set(pid()),
    shutdowns :: gb_sets:set(shutdown()),
    atomics :: atomics:atomics_ref(),
    %
    %probe_tag :: none | {some, term()},
    probing_timer :: none | reference(),
    probes :: [probe()],
    missed_probes :: non_neg_integer()
}).
-type state() :: #state{}.

-record(settings, {
    min_workers :: pos_integer(),
    max_workers :: pos_integer()
}).
-type settings() :: #settings{}.

-record(worker, {
    pid :: pid(),
    start_ts :: timestamp(),
    ready_ts :: none | timestamp(),
    auto_retires :: boolean(),
    retire_ts :: none | timestamp(),
    shutdown_ts :: none | timestamp()
}).
-type worker() :: #worker{}.

-type shutdown() :: nonempty_improper_list(timestamp(), pid()).

-record(probe, {
    ts :: timestamp(),
    tag :: term(),
    sojourn_time :: none | non_neg_integer(),
    cancelled :: boolean()
}).
-type probe() :: #probe{}.

-type timestamp() :: integer().

%%

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?SERVER,
        start => {?MODULE, start_link, []}
    }.

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_broker() ->
    persistent_term:get({ahhh, ?SERVER}).

run(Min, Max) ->
    Broker = persistent_term:get({ahhh, ?SERVER}),

    case cbroker_nif:ask(Broker, left, {self(), {sleep_between, Min, Max}}, false) of
        retry ->
            run(Min, Max);
        %
        {await, Tag} ->
            receive
                {Tag, MatchResult} ->
                    case MatchResult of
                        cancelled ->
                            run(Min, Max);
                        %
                        {match, MatchRef, WorkerPid} ->
                            WorkerMon = monitor(process, WorkerPid),

                            receive
                                {MatchRef, Result} ->
                                    demonitor(WorkerMon, [flush]),
                                    Result;
                                %
                                {'DOWN', WorkerMon, process, Pid, Reason} ->
                                    error({worker_stopped, Pid, Reason})
                            end
                    end
            end;
        %
        {match, MatchRef, WorkerPid} ->
            WorkerMon = monitor(process, WorkerPid),

            receive
                {MatchRef, Result} ->
                    demonitor(WorkerMon, [flush]),
                    Result;
                %
                {'DOWN', WorkerMon, process, Pid, Reason} ->
                    error({worker_stopped, Pid, Reason})
            end
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-spec init([]) -> {ok, state()}.
init([]) ->
    _ = process_flag(trap_exit, true),

    Settings = #settings{
        min_workers = 4,
        max_workers = 1000
    },

    Broker = cbroker_nif:new(),
    persistent_term:put({ahhh, ?SERVER}, Broker),

    State = #state{
        settings = Settings,
        broker = Broker,
        workers = #{},
        retirees = gb_sets:new(),
        shutdowns = gb_sets:new(),
        atomics = atomics:new(2, [{signed, false}]),
        %
        probing_timer = none,
        probes = []
    },

    State2 = start_missing_workers(State),
    State3 = State2#state{probing_timer = schedule_probing()},
    {ok, State3}.

%-spec handle_call(Request, From, State) -> {stop, Reason, State} when
%    Request :: term(),
%    From :: gen_server:from(),
%    State :: state(),
%    Reason :: {unexpected_call, #{request := term(), from := gen_server:from()}}.
handle_call(Request, From, State) ->
    ErrorDetails = #{request => Request, from => From},
    {stop, {unexpected_call, ErrorDetails}, State}.

%-spec handle_cast(Request, State) -> {stop, {unexpected_cast, term()}, State} when
%    Request :: term(),
%    State :: state().
handle_cast(Request, State) ->
    {stop, {unexpected_cast, Request}, State}.

%-spec handle_info(Info, State) -> {stop, {unexpected_info, term()}, State} when
%    Info :: term(),
%    State :: state().
handle_info({worker_ready, Pid}, State) ->
    handle_worker_ready(Pid, State);
handle_info({worker_retired, Pid, ShutdownTs}, State) ->
    handle_worker_retirement(Pid, ShutdownTs, State);
handle_info(refresh_probing, State) ->
    false = erlang:cancel_timer(State#state.probing_timer),
    State2 = State#state{probing_timer = schedule_probing()},
    State3 = refresh_probing(State2),
    State4 = drop_old_probes(State3),
    State5 = react(State4),
    {noreply, State5};
handle_info({Tag, Reply}, State) ->
    handle_tagged_reply(Tag, Reply, State);
handle_info({'EXIT', Pid, Reason}, State) ->
    handle_linked_process_exit(Pid, Reason, State);
handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state() | term(), term()) ->
    {ok, state()} | {error, {cannot_convert_state, term()}}.
code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State};
code_change(_OldVsn, State, _Extra) ->
    {error, {cannot_convert_state, State}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

start_missing_workers(#state{settings = Settings} = State) ->
    start_missing_workers(Settings, State).

start_missing_workers(
    #settings{min_workers = MinWorkers} = Settings, #state{workers = Workers} = State
) when
    map_size(Workers) < MinWorkers
->
    UpdatedState = start_worker(false, State),
    start_missing_workers(Settings, UpdatedState);
start_missing_workers(#settings{}, #state{} = State) ->
    State.

start_worker(AutoRetire, State) ->
    ?assertEqual([], gb_sets:to_list(State#state.retirees)),
    ?assertEqual([], gb_sets:to_list(State#state.shutdowns)),

    logger:notice("Expanding pool to ~p", [map_size(State#state.workers) + 1]),

    Args = #{
        broker => State#state.broker,
        cb => cbroker_testworker1,
        cb_args => [todo],
        auto_retire => AutoRetire,
        atomics => State#state.atomics
    },
    {ok, Pid} = cbroker_worker:start_link(Args),

    Worker = #worker{
        pid = Pid,
        start_ts = timestamp_now(),
        ready_ts = none,
        auto_retires = AutoRetire,
        retire_ts = none,
        shutdown_ts = none
    },

    UpdatedWorkers = (State#state.workers)#{Pid => Worker},

    State#state{workers = UpdatedWorkers}.

%%%

handle_worker_ready(Pid, #state{workers = Workers} = State) ->
    Worker = #worker{ready_ts = none} = maps:get(Pid, Workers),
    UpdatedWorker = Worker#worker{ready_ts = timestamp_now()},
    UpdatedWorkers = Workers#{Pid := UpdatedWorker},
    UpdatedState = State#state{workers = UpdatedWorkers},
    {noreply, UpdatedState}.

handle_worker_retirement(Pid, ShutdownTs, #state{workers = Workers, retirees = Retirees, shutdowns = Shutdowns} = State) ->
    Worker = #worker{} = maps:get(Pid, Workers),
    ?assertEqual(none, Worker#worker.retire_ts),

    UpdatedWorker = Worker#worker{
        retire_ts = timestamp_now(),
        shutdown_ts = ShutdownTs
    },
    UpdatedWorkers = Workers#{Pid := UpdatedWorker},
    UpdatedRetirees = gb_sets:insert(Pid, Retirees),

    Shutdown = [ShutdownTs | Pid],
    UpdatedShutdowns = gb_sets:insert(Shutdown, Shutdowns),

    logger:notice("Shrinking active pool to ~p (~p)", [map_size(UpdatedWorkers) - gb_sets:size(UpdatedRetirees), Pid]),

    UpdatedState = State#state{
                     workers = UpdatedWorkers, 
                     retirees = UpdatedRetirees,
                     shutdowns = UpdatedShutdowns
                    },
    {noreply, UpdatedState}.

%%%

handle_linked_process_exit(Pid, _Reason, #state{workers = Workers} = State) ->
    case maps:take(Pid, Workers) of
        {#worker{} = Worker, Remaining} ->
            handle_worker_exit(Pid, Worker, Remaining, State);
        %
        error ->
            {noreply, State}
    end.

handle_worker_exit(Pid, Worker, Remaining, #state{retirees = Retirees, shutdowns = Shutdowns} = State) ->
    {UpdatedRetirees, UpdatedShutdowns} =
        case Worker#worker.retire_ts of
            none ->
                logger:notice("Shrinking pool to ~p (DIDNT RETIRE)", [map_size(Remaining)]),
                {Retirees, Shutdowns};
            %
            _ ->
                %logger:notice("Retired worker ~p terminated", [Pid]),
                ShutdownTs = Worker#worker.shutdown_ts,
                ?assertNotEqual(none, ShutdownTs),
                Shutdown = [ShutdownTs | Pid],
                {
                 gb_sets:delete(Pid, Retirees),
                 gb_sets:delete(Shutdown, Shutdowns)
                }
        end,


    State2 = State#state{
               workers = Remaining, 
               retirees = UpdatedRetirees,
               shutdowns = UpdatedShutdowns
              },

    State3 = start_missing_workers(State2),
    {noreply, State3}.

%%%

timestamp_now() ->
    erlang:monotonic_time(native).

%%%


schedule_probing() ->
    erlang:send_after(?PROBING_INTERVAL, self(), refresh_probing).

refresh_probing(#state{probes = [#probe{ts = Ts, tag = Tag, sojourn_time = none} = Probe | NextProbes]} = State) ->
    _ = cbroker_nif:cancel(State#state.broker, Tag),
    SojournTime = timestamp_now() - Ts,
    UpdatedProbe = Probe#probe{sojourn_time = SojournTime, cancelled = true},
    UpdatedState = State#state{
        probes = [UpdatedProbe | NextProbes], 
        missed_probes = State#state.missed_probes + 1
    },
    refresh_probing(UpdatedState);
refresh_probing(#state{broker = Broker} = State) ->
    case cbroker_nif:ask(Broker, left, probe, true, false) of
        {await, Tag} ->
            Probe = #probe{ts = timestamp_now(), tag = Tag, sojourn_time = none, cancelled = false},
            State#state{probes = [Probe | State#state.probes]};
        %
        {match, _, _, SojournTime} ->
            Probe = #probe{ts = timestamp_now(), tag = none, sojourn_time = SojournTime, cancelled = false},
            State#state{probes = [Probe | State#state.probes], missed_probes = 0};
        %
        retry ->
            refresh_probing(State)
    end.

handle_tagged_reply(Tag, Reply, #state{probes = [#probe{tag = Tag, sojourn_time = none} = Probe | NextProbes]} = State) ->
    ?assertEqual(none, Probe#probe.sojourn_time),

    SojournTime =
        case Reply of
            {match, _, _, SojournT} ->
                SojournT;
            %
            cancelled ->
                % Good enough approximation
                timestamp_now() - Probe#probe.ts
        end,

    UpdatedProbe = Probe#probe{sojourn_time = SojournTime},
    State2 = State#state{probes = [UpdatedProbe | NextProbes], missed_probes = 0},
    {noreply, State2};
handle_tagged_reply(_Tag, _Reply, #state{probes = [#probe{} | _]} = State) ->
    % Too late
    {noreply, State}.

drop_old_probes(#state{probes = Probes} = State) ->
    State#state{probes = lists:sublist(Probes, ceil(1_000 / ?PROBING_INTERVAL))}.

react(#state{missed_probes = 0} = State) ->
    State;
react(#state{workers = Workers, retirees = Retirees} = State) ->
    #settings{max_workers = MaxWorkers} = State#state.settings,

    N = map_size(Workers) - gb_sets:size(Retirees),
    NewN = min(MaxWorkers, max(N +1, floor(1.07 * N))),
    ?assertMatch(_ when NewN >= N, {NewN, N}),

    more_workers(NewN - N, State).


more_workers(Amount, #state{shutdowns = Shutdowns} = State) ->
    MinShutdownTs = timestamp_now() + 50,

    ResumptionIter = gb_sets:iterator_from([MinShutdownTs], Shutdowns),

    more_workers_recur(Amount, gb_sets:next(ResumptionIter), State).

more_workers_recur(Amount, {Shutdown, ResumptionIter}, State) when Amount > 0 ->
    [ShutdownTs | Pid] = Shutdown,

    Worker = #worker{} = maps:get(Pid, State#state.workers),
    ?assertNotEqual(none, Worker#worker.retire_ts),
    ?assertNotEqual(none, Worker#worker.shutdown_ts),
    ?assertEqual(ShutdownTs, Worker#worker.shutdown_ts),

    logger:notice("Resuming worker ~p (size ~p)", [Pid, map_size(State#state.workers) - gb_sets:size(State#state.retirees) + 1]),
    _ = Pid ! resume,

    UpdatedWorker = Worker#worker{retire_ts = none, shutdown_ts = none},
    UpdatedWorkers = (State#state.workers)#{Pid := UpdatedWorker},
    RemainingRetirees = gb_sets:delete(Pid, State#state.retirees),
    RemainingShutdowns = gb_sets:delete(Shutdown, State#state.shutdowns),

    UpdatedState = State#state{
        workers = UpdatedWorkers,
        retirees = RemainingRetirees,
        shutdowns = RemainingShutdowns
    },

    Next = gb_sets:next(ResumptionIter),
    more_workers_recur(Amount - 1, Next, UpdatedState);
more_workers_recur(Amount, none, State) when Amount > 0 ->
    AutoRetire = map_size(State#state.workers) >= (State#state.settings)#settings.min_workers,
    UpdatedState = start_worker(AutoRetire, State),
    more_workers_recur(Amount - 1, none, UpdatedState);
more_workers_recur(0, _, State) ->
    State.

% count_in_flight(Workers) ->
%     List = maps:values(Workers),
%     count_in_flight_recur(List).
% 
% count_in_flight_recur([#worker{ready_ts = ReadyTs} | Next]) ->
%     case ReadyTs =:= none of
%         true ->
%             1 + count_in_flight_recur(Next);
%         %
%         false ->
%             count_in_flight_recur(Next)
%     end;
% count_in_flight_recur([]) ->
%     0.
