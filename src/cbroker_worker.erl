-module(cbroker_worker).

-ifdef(E48).
-moduledoc false.
-endif.

-include_lib("stdlib/include/assert.hrl").

%% ------------------------------------------------------------------
%% Callback Definitions
%% ------------------------------------------------------------------

-callback start_link(Args) -> {ok, pid()} | {error, term()} when
    Args :: term().

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    start_link/1,
    checkin/1
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

%% ------------------------------------------------------------------
%% Record and Type Definitions
%% ------------------------------------------------------------------

-type init_args() :: #{
    broker := cbroker_nif:broker(),
    cb := module(),
    cb_args := term()
}.
-export_type([init_args/0]).

%%

-record(state, {
    invariants :: invariants(),
    status :: status()
}).
-type state() :: #state{}.

-record(invariants, {
    parent :: pid(),
    broker :: reference(),
    cb :: module(),
    cb_pid :: pid(),
    cb_mon :: reference()
}).
-type invariants() :: #invariants{}.

-type status() ::
    (idle
    | {waiting, cbroker_nif:tag()}
    | {checked_out, pid(), Monitor :: reference()}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec start_link(init_args()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link(Args) ->
    proc_lib:start_link(?MODULE, init, [self(), Args]).

-spec checkin(pid()) -> ok.
checkin(Pid) ->
    _ = Pid ! {checkin, self()},
    ok.

%% ------------------------------------------------------------------
%% sys Function Definitions
%% ------------------------------------------------------------------

-spec init(pid(), init_args()) -> no_return().
init(Parent, Args) ->
    #{
        broker := Broker,
        cb := Cb,
        cb_args := CbArgs
    } = Args,

    Debug = sys:debug_options([]),
    proc_lib:init_ack(Parent, {ok, self()}),

    {ok, Pid} = cb_start_link(Cb, CbArgs),
    _ = Parent ! {worker_ready, self()},

    Invariants = #invariants{
        parent = Parent,
        broker = Broker,
        cb = Cb,
        cb_pid = Pid,
        cb_mon = monitor(process, Pid)
    },

    State = #state{
        invariants = Invariants,
        status = idle
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
%% Callback Invokers
%% ------------------------------------------------------------------

cb_start_link(Cb, Args) ->
    Cb:start_link(Args).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

state_parent(#state{invariants = #invariants{parent = Parent}}) ->
    Parent.

loop(Debug, #state{status = idle} = State) ->
    receive
        Msg ->
            handle_msg(Msg, Debug, State)
    after 0 ->
        UpdatedState = ask(State),
        loop(Debug, UpdatedState)
    end;
loop(Debug, #state{} = State) ->
    receive
        Msg ->
            UpdatedState = handle_msg(Msg, Debug, State),
            loop(Debug, UpdatedState)
    end.

%%

handle_msg({system, From, Request}, Debug, State) ->
    Parent = state_parent(State),
    sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
handle_msg(Msg, Debug, State) ->
    UpdatedDebug = sys:handle_debug(Debug, fun ?MODULE:write_debug/3, ?MODULE, {in, Msg}),
    UpdatedState = handle_non_system_msg(Msg, State),
    loop(UpdatedDebug, UpdatedState).

-spec handle_non_system_msg(term(), state()) -> state() | no_return().
handle_non_system_msg(Msg, #state{status = idle} = State) ->
    handle_unexpected_msg(Msg, State);
handle_non_system_msg(Msg, #state{status = {waiting, Tag}} = State) ->
    case Msg of
        {Tag, {match, _MatchRef, {checkout, CallerPid}}} ->
            Mon = monitor(process, CallerPid),
            State#state{status = {checked_out, CallerPid, Mon}};
        %
        _ ->
            handle_unexpected_msg(Msg, State)
    end;
handle_non_system_msg(Msg, #state{status = {checked_out, Pid, Mon}} = State) ->
    case Msg of
        {checkin, Pid} ->
            demonitor(Mon),
            State#state{status = idle};
        %
        {'DOWN', Mon, _, _, _} ->
            State#state{status = idle};
        %
        _ ->
            handle_unexpected_msg(Msg, State)
    end.

handle_unexpected_msg({'DOWN', Mon, process, _, _}, State) ->
    case State#state.invariants of
        #invariants{cb_mon = Mon} ->
            terminate(normal);
        _ ->
            % Late monitor
            State
    end;
handle_unexpected_msg(Msg, State) ->
    logger:notice("Unexpected info: ~p", [Msg]),
    State.

-spec terminate(term()) -> no_return().
terminate(Reason) ->
    exit(Reason).

%%

ask(#state{invariants = Invariants} = State) ->
    #invariants{broker = Broker, cb_pid = CbPid} = Invariants,
    ?assertEqual(idle, State#state.status),

    ExchangeValue = {self(), CbPid},

    logger:notice("ask!!! ~p", [self()]),

    case cbroker_nif:ask(Broker, right, ExchangeValue, false) of
        {await, Tag} ->
            State#state{status = {waiting, Tag}};
        %
        {match, _MatchRef, {checkout, CallerPid}} ->
            Mon = monitor(process, CallerPid),
            State#state{status = {checked_out, CallerPid, Mon}};
        %
        retry ->
            State
    end.
