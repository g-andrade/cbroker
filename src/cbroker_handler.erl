-module(cbroker_handler).

-ifdef(E48).
-moduledoc false.
-endif.

-include_lib("stdlib/include/assert.hrl").

%% ------------------------------------------------------------------
%% Callback Definitions
%% ------------------------------------------------------------------

-callback init(Args) -> init_ret() when
    Args :: term().

-callback handle_request(Request, From, State) -> handle_request_ret(State) when
    Request :: term(),
    From :: from().

-callback handle_requester_down(From, Reason, State) -> handle_requester_down_ret(State) when
    From :: from(),
    Reason :: term().

-callback handle_info(Info, State) -> handle_info_ret(State) when
    Info :: term().

-optional_callbacks([
    handle_requester_down/3,
    handle_info/2
]).

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

%% ------------------------------------------------------------------
%% Record and Type Definitions
%% ------------------------------------------------------------------

-type init_args() :: #{
    broker := cbroker_nif:broker(),
    cb := module(),
    cb_args := term()
}.
-export_type([init_args/0]).

-type init_ret() :: {ok, CbState :: term()}.
-export_type([init_ret/0]).

-type from() :: {pid(), cbroker_nif:match_ref()}.
-export_type([from/0]).

% TODO
-type handle_request_ret(State) ::
    ({reply, Reply :: term(), State}
    | {reply_later, State}
    | {slot_take, State}
    | {stream_start, Msg :: term(), State}).
-export_type([handle_request_ret/1]).

-type handle_requester_down_ret(State) ::
    ({cancelled, State}
    | {cancelling, State}).
-export_type([handle_requester_down_ret/1]).

-type handle_info_ret(State) ::
    ({cancelled, from(), State}
    | {noreply, State}
    | {reply, from(), Msg :: term(), State}
    | {slot_return, State}
    | {stream, from(), Msg :: term(), State}).
-export_type([handle_info_ret/1]).

%%

-record(state, {
    invariants :: invariants(),
    cb_state :: term(),
    slots :: non_neg_integer(),
    waiters :: #{cbroker_nif:tag() => waiting},
    jobs :: #{cbroker_nif:match_ref() => job()},
    mons :: #{reference() => cbroker_nif:match_ref()}
}).
-type state() :: #state{}.

-record(invariants, {
    parent :: pid(),
    broker :: reference(),
    cb :: module()
}).
-type invariants() :: #invariants{}.

-record(job, {
    requester_pid :: pid(),
    requester_mon :: reference()
}).
-type job() :: #job{}.

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
        cb_args := CbArgs
    } = Args,

    Debug = sys:debug_options([]),
    proc_lib:init_ack(Parent, {ok, self()}),

    {ok, CbState} = cb_init(Cb, CbArgs),
    _ = Parent ! {worker_ready, self()},

    Invariants = #invariants{
        parent = Parent,
        broker = Broker,
        cb = Cb
    },

    State = #state{
        invariants = Invariants,
        cb_state = CbState,
        slots = 1,
        waiters = #{},
        jobs = #{},
        mons = #{}
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

-spec cb_init(module(), term()) -> init_ret().
cb_init(Cb, Args) ->
    Cb:init(Args).

-spec cb_handle_request(module(), term(), from(), State) -> handle_request_ret(State).
cb_handle_request(Cb, Request, From, CbState) ->
    Cb:handle_request(Request, From, CbState).

-spec cb_handle_requester_down(module(), from(), term(), State) -> handle_requester_down_ret(State).
cb_handle_requester_down(Cb, From, Reason, CbState) ->
    Cb:handle_requester_down(From, Reason, CbState).

-spec cb_handle_info(module(), term(), State) -> handle_info_ret(State).
cb_handle_info(Cb, Info, CbState) ->
    Cb:handle_info(Info, CbState).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

state_parent(#state{invariants = #invariants{parent = Parent}}) ->
    Parent.

loop(Debug, #state{slots = Slots} = State) when
    Slots > 0
->
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
handle_non_system_msg(Msg, #state{waiters = Waiters, mons = Mons} = State) ->
    case Msg of
        {Tag, Content} when is_map_key(Tag, Waiters) ->
            RemainingWaiters = maps:remove(Tag, Waiters),
            UpdatedState = State#state{waiters = RemainingWaiters},
            handle_async_ask_reply(Content, UpdatedState);
        %
        {'DOWN', Mon, process, Pid, Reason} when is_map_key(Mon, Mons) ->
            {MatchRef, RemainingMons} = maps:take(Mon, Mons),
            UpdatedState = State#state{mons = RemainingMons},
            handle_requester_down(Pid, MatchRef, Reason, UpdatedState);
        %
        _ ->
            handle_info(Msg, State)
    end.

-spec terminate(term()) -> no_return().
terminate(Reason) ->
    exit(Reason).

%%

ask(#state{invariants = Invariants, slots = Slots, waiters = Waiters} = State) ->
    #invariants{broker = Broker} = Invariants,
    ?assertMatch(_ when Slots > 0, Slots),

    logger:notice("WORKER ASKING!! (~p)", [map_size(Waiters) + 1]),

    case cbroker_nif:ask(Broker, right, self(), false) of
        {await, Tag} ->
            UpdatedWaiters = Waiters#{Tag => waiting},
            State#state{slots = Slots - 1, waiters = UpdatedWaiters};
        %
        {match, MatchRef, ExchangeValue} ->
            UpdatedState = State#state{slots = Slots - 1},
            handle_match(MatchRef, ExchangeValue, UpdatedState);
        %
        retry ->
            State
    end.

%%

handle_match(
    MatchRef,
    {Pid, Request},
    #state{
        invariants = Invariants,
        cb_state = CbState,
        slots = Slots
    } = State
) ->
    #invariants{cb = Cb} = Invariants,
    From = {Pid, MatchRef},

    case cb_handle_request(Cb, Request, From, CbState) of
        {reply, Reply, UpdatedCbState} ->
            _ = Pid ! {MatchRef, Reply},
            State#state{
                cb_state = UpdatedCbState,
                slots = Slots + 1
            };
        %
        {reply_later, UpdatedCbState} ->
            UpdatedState = State#state{cb_state = UpdatedCbState},
            job_start(MatchRef, Pid, UpdatedState);
        %
        {stream_start, StreamMsg, UpdatedCbState} ->
            _ = Pid ! {MatchRef, StreamMsg},
            UpdatedState = State#state{cb_state = UpdatedCbState},
            job_start(MatchRef, Pid, UpdatedState);
        %
        %
        {slot_take, UpdatedCbState} ->
            State#state{cb_state = UpdatedCbState}
    end.

%%

handle_async_ask_reply(Content, #state{} = State) ->
    case Content of
        {match, MatchRef, ExchangeValue} ->
            handle_match(MatchRef, ExchangeValue, State);
        %
        cancelled ->
            State#state{slots = State#state.slots + 1}
    end.

%%

handle_info(Info, #state{invariants = Invariants, cb_state = CbState} = State) ->
    #invariants{cb = Cb} = Invariants,

    case cb_handle_info(Cb, Info, CbState) of
        {noreply, UpdatedCbState} ->
            State#state{cb_state = UpdatedCbState};
        %
        {cancelled, From, UpdatedCbState} ->
            UpdatedState = State#state{cb_state = UpdatedCbState},
            job_cancelled(From, UpdatedState);
        %
        {stream, From, StreamMsg, UpdatedCbState} ->
            UpdatedState = State#state{cb_state = UpdatedCbState},
            job_msg(From, StreamMsg, UpdatedState);
        %
        {reply, From, Reply, UpdatedCbState} ->
            UpdatedState = State#state{cb_state = UpdatedCbState},
            job_done(From, Reply, UpdatedState);
        %
        {slot_return, UpdatedCbState} ->
            State#state{
                cb_state = UpdatedCbState,
                slots = State#state.slots + 1
            }
    end.

%%

handle_requester_down(
    Pid,
    MatchRef,
    Reason,
    #state{
        invariants = Invariants,
        cb_state = CbState,
        jobs = Jobs
    } = State
) ->
    #invariants{cb = Cb} = Invariants,

    {Job, RemainingJobs} = maps:get(MatchRef, Jobs),
    #job{requester_pid = Pid} = Job,
    From = {Pid, MatchRef},

    case cb_handle_requester_down(Cb, From, Reason, CbState) of
        {cancelling, UpdatedCbState} ->
            State#state{
                cb_state = UpdatedCbState,
                jobs = RemainingJobs
            };
        %
        {cancelled, UpdatedCbState} ->
            State#state{
                cb_state = UpdatedCbState,
                slots = State#state.slots + 1,
                jobs = RemainingJobs
            }
    end.

%%

job_start(MatchRef, Pid, #state{jobs = Jobs, mons = Mons} = State) ->
    ?assertEqual(false, is_map_key(MatchRef, Jobs)),
    Mon = monitor(process, Pid),
    Job = #job{requester_pid = Pid, requester_mon = Mon},
    UpdatedJobs = Jobs#{MatchRef => Job},
    UpdatedMons = Mons#{Mon => MatchRef},

    State#state{
        jobs = UpdatedJobs,
        mons = UpdatedMons
    }.

job_msg({Pid, MatchRef}, StreamMsg, #state{jobs = Jobs} = State) ->
    #job{requester_pid = Pid} = maps:get(MatchRef, Jobs),
    _ = Pid ! {MatchRef, StreamMsg},
    State.

job_cancelled({MatchRef, _Pid} = From, #state{slots = Slots, jobs = Jobs} = State) ->
    case maps:is_key(MatchRef, Jobs) of
        true ->
            job_done(From, cancelled, State);
        %
        false ->
            State#state{slots = Slots + 1}
    end.

job_done({Pid, MatchRef}, FinalMsg, #state{slots = Slots, jobs = Jobs, mons = Mons} = State) ->
    {Job, RemainingJobs} = maps:take(MatchRef, Jobs),
    #job{requester_pid = Pid, requester_mon = Mon} = Job,

    case maps:take(Mon, Mons) of
        {MatchRef, RemainingMons} ->
            demonitor(Mon, [flush]),
            _ = Pid ! {MatchRef, FinalMsg},
            State#state{
                slots = Slots + 1,
                jobs = RemainingJobs,
                mons = RemainingMons
            };
        %
        error ->
            % Requester already terminated
            State#state{
                slots = Slots + 1,
                jobs = RemainingJobs
            }
    end.
