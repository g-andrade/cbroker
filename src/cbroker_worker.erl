-module(cbroker_worker).

-ifdef(E48).
-moduledoc false.
-endif.

-include_lib("stdlib/include/assert.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    start_link/1
]).

%% ------------------------------------------------------------------
%% sys Function Exports
%% ------------------------------------------------------------------

-export([
    init/2,
    system_code_change/4,
    system_continue/3,
    system_terminate/4,
    write_debug/3
]).

-ignore_xref([
    init/2,
    system_code_change/4,
    system_continue/3,
    system_terminate/4,
    write_debug/3
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(IDLE_TIMEOUT, 150).

%% ------------------------------------------------------------------
%% Record and Type Definitions
%% ------------------------------------------------------------------

-type init_args() :: #{
    broker := reference(),
    cb := module(),
    cb_args := term(),
    auto_retire := boolean(),
    atomics := atomics:atomics_ref()
}.
-export_type([init_args/0]).

-record(state, {
    invariants :: invariants(),
    cb_state :: term(),
    status :: idle | {waiting, Tag :: term(), Timer :: reference()} | working | {retired, reference(), reference()},
    last_probe_sojourn :: none | non_neg_integer()
}).
-type state() :: #state{}.

-record(invariants, {
    parent :: pid(),
    broker :: reference(),
    cb :: module(),
    auto_retire :: boolean(),
    atomics :: atomics:atomics_ref()
}).
-type invariants() :: #invariants{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec start_link(init_args()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link(Args) ->
    proc_lib:start_link(?MODULE, init, [self(), Args]).

%% ------------------------------------------------------------------
%% sys Function Definitions
%% ------------------------------------------------------------------

-spec init(pid(), init_args()) -> no_return().
init(Parent, Args) ->
    #{
        broker := Broker,
        cb := Cb,
        cb_args := CbArgs,
        auto_retire := AutoRetire,
        atomics := Atomics
    } = Args,

    Debug = sys:debug_options([]),
    proc_lib:init_ack(Parent, {ok, self()}),

    {ok, CbState} = Cb:init(CbArgs),
    _ = Parent ! {worker_ready, self()},

    Invariants = #invariants{
        parent = Parent,
        broker = Broker,
        cb = Cb,
        auto_retire = AutoRetire,
        atomics = Atomics
    },

    State = #state{
        invariants = Invariants,
        cb_state = CbState,
        status = idle,
        last_probe_sojourn = none
    },
    loop(Debug, State).

-spec write_debug(io:device(), term(), term()) -> ok.
write_debug(Dev, Event, Name) ->
    % called by sys:handle_debug().
    io:format(Dev, "~p event = ~p~n", [Name, Event]).

-spec system_continue(pid(), [sys:dbg_opt()], state()) -> no_return().
system_continue(Parent, Debug, State) ->
    % https://www.erlang.org/doc/apps/stdlib/sys.html#Module:system_continue-3
    ?assertEqual(state_parent(State), Parent),
    loop(Debug, State).

-spec system_terminate(term(), pid(), [sys:dbg_opt()], state()) -> no_return().
system_terminate(Reason, Parent, _Debug, State) ->
    % https://www.erlang.org/doc/apps/stdlib/sys.html#Module:system_terminate-4
    ?assertEqual(state_parent(State), Parent),
    terminate(Reason).

-spec system_code_change(state(), ?MODULE, term(), term()) -> {ok, state()}.
system_code_change(#state{} = State, _Module, _OldVsn, _Extra) ->
    % https://www.erlang.org/doc/apps/stdlib/sys.html#Module:system_code_change-4
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

state_parent(#state{invariants = #invariants{parent = Parent}}) ->
    Parent.

loop(Debug, #state{status = idle, invariants = Invariants} = State) ->
    receive
        Msg ->
            handle_msg(Msg, Debug, State)
    after 0 ->
        #invariants{broker = Broker} = Invariants,

        case cbroker_nif:ask(Broker, right, self(), true) of
            retry ->
                loop(Debug, State);
            %
            {await, Tag} ->
                Timeout = idle_timeout(State),
                Timer = maybe_start_match_timer(Timeout, Tag),
                UpdatedState = State#state{status = {waiting, Tag, Timer}},
                loop(Debug, UpdatedState);
            %
            MatchResult ->
                UpdatedState = handle_match_result(MatchResult, State),
                loop(Debug, UpdatedState)
        end
    end;
loop(Debug, #state{} = State) ->
    receive
        Msg ->
            UpdatedState = handle_msg(Msg, Debug, State),
            loop(Debug, UpdatedState)
    end.

%%

idle_timeout(#state{invariants = #invariants{auto_retire = AutoRetire}, last_probe_sojourn = LastProbeSojourn}) ->
    case AutoRetire of
        true when LastProbeSojourn =:= none ->
            ?IDLE_TIMEOUT;
        %
        true ->
            max(0, ?IDLE_TIMEOUT - erlang:convert_time_unit(LastProbeSojourn, native, millisecond));
            %10;
        %
        _ ->
            infinity
    end.

maybe_start_match_timer(infinity, _) ->
    none;
maybe_start_match_timer(Timeout, Tag) ->
    erlang:send_after(Timeout, self(), {timeout, Tag}).

maybe_cancel_timer(none) -> false;
maybe_cancel_timer(Timer) -> erlang:cancel_timer(Timer).

%%

handle_match_timeout(#state{status = {waiting, Tag, Timer}} = State) ->
    false = maybe_cancel_timer(Timer),
    #invariants{broker = Broker} = State#state.invariants,

    case cbroker_nif:cancel(Broker, Tag) of
        cancelled ->
            ShutdownRef = make_ref(),
            ShutdownTimeout = 5_000 + rand:uniform(10_000),
            ScheduledShutdownTs = erlang:monotonic_time(native) + erlang:convert_time_unit(ShutdownTimeout, millisecond, native),
            ShutdownTimer = erlang:send_after(ShutdownTimeout, self(), {shutdown, ShutdownRef}),
            state_parent(State) ! {worker_retired, self(), ScheduledShutdownTs},
            State#state{status = {retired, ShutdownTimer, ShutdownRef}};
        %
        too_late ->
            State
    end.

handle_match_result({match, MatchRef, Request, SojournTime}, State) ->
    %logger:notice("SojournTime is ~p ms", [erlang:convert_time_unit(SojournTime, native, millisecond)]),
    #invariants{atomics = Atomics} = State#state.invariants,
    atomics:add(Atomics, 1, SojournTime),
    atomics:add(Atomics, 2, 1),
    State2 = State#state{status = working},
    handle_request(MatchRef, Request, SojournTime, State2);
handle_match_result(cancelled, State) ->
    State#state{status = idle}.

handle_request(MatchRef, {Pid, Work}, _, #state{cb_state = CbState} = State) ->
    #invariants{cb = Cb, atomics = Atomics} = State#state.invariants,

    case Cb:handle_work(Work, {Pid, MatchRef}, CbState) of
        {reply, Reply, UpdatedCbState} ->
            atomics:add(Atomics, 2, -1),
            _ = Pid ! {MatchRef, Reply},
            State#state{
              cb_state = UpdatedCbState, 
              status = idle,
              last_probe_sojourn = none
             }
        %
        % {error, _} ->
        %
    end;
handle_request(_MatchRef, probe, SojournTime, State) ->
    State#state{
      status = idle,
      last_probe_sojourn = SojournTime
     }.

%%

handle_msg({system, From, Request}, Debug, State) ->
    Parent = state_parent(State),
    sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
handle_msg(Msg, Debug, State) ->
    UpdatedDebug = sys:handle_debug(Debug, fun ?MODULE:write_debug/3, ?MODULE, {in, Msg}),
    UpdatedState = handle_non_system_msg(Msg, State),
    loop(UpdatedDebug, UpdatedState).

-spec handle_non_system_msg(term(), state()) -> state() | no_return().
handle_non_system_msg({Tag, Result}, #state{status = {waiting, Tag, Timer}} = State) ->
    _ = maybe_cancel_timer(Timer),
    handle_match_result(Result, State);
handle_non_system_msg({timeout, Tag}, #state{status = {waiting, Tag, _Timer}} = State) ->
    handle_match_timeout(State);
handle_non_system_msg({timeout, _Tag}, #state{} = State) ->
    % late timeout
    State;
handle_non_system_msg({shutdown, Ref}, State) ->
    case State#state.status of
        {retired, Timer, Ref} ->
            false = erlang:cancel_timer(Timer),
            exit(normal);
        %
        _ ->
            % late shutdown msg
            State
    end;
handle_non_system_msg(resume, State) ->
    {retired, Timer, _} = State#state.status,
    _ = erlang:cancel_timer(Timer),
    State#state{status = idle};
handle_non_system_msg(Msg, _State) ->
    terminate({unexpected_msg, Msg}).

-spec terminate(term()) -> no_return().
terminate(Reason) ->
    exit(Reason).
