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

%% ------------------------------------------------------------------
%% Record and Type Definitions
%% ------------------------------------------------------------------

-type init_args() :: #{
    cb := module(),
    cb_args := term()
}.
-export_type([init_args/0]).

-record(state, {
    parent :: pid(),
    broker :: reference(),
    cb :: module(),
    cb_state :: term(),
    status :: idle | {waiting, Tag :: term()} | working
}).
-type state() :: #state{}.

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

    {ok, CbState} = Cb:init(CbArgs),
    _ = Parent ! {worker_ready, self()},

    State = #state{
        parent = Parent,
        broker = Broker,
        cb = Cb,
        cb_state = CbState,
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
    ?assertEqual(State#state.parent, Parent),
    loop(Debug, State).

-spec system_terminate(term(), pid(), [sys:dbg_opt()], state()) -> no_return().
system_terminate(Reason, Parent, _Debug, State) ->
    % https://www.erlang.org/doc/apps/stdlib/sys.html#Module:system_terminate-4
    ?assertEqual(State#state.parent, Parent),
    terminate(Reason).

-spec system_code_change(state(), ?MODULE, term(), term()) -> {ok, state()}.
system_code_change(#state{} = State, _Module, _OldVsn, _Extra) ->
    % https://www.erlang.org/doc/apps/stdlib/sys.html#Module:system_code_change-4
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

loop(Debug, #state{status = idle, broker = Broker} = State) ->
    receive
        Msg ->
            handle_msg(Msg, Debug, State)
    after 0 ->
        case cbroker_nif:ask(Broker, right, self(), true) of
            retry ->
                loop(Debug, State);
            %
            {await, Tag} ->
                UpdatedState = State#state{status = {waiting, Tag}},
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
            handle_msg(Msg, Debug, State)
    end.

%%

handle_match_result({match, MatchRef, Request, _SojournTime}, State) ->
    State2 = State#state{status = working},
    handle_request(MatchRef, Request, State2);
handle_match_result(cancelled, State) ->
    State#state{status = idle}.

handle_request(MatchRef, {Pid, Work}, #state{cb = Cb, cb_state = CbState} = State) ->
    case Cb:handle_work(Work, {Pid, MatchRef}, CbState) of
        {reply, Reply, UpdatedCbState} ->
            _ = Pid ! {MatchRef, Reply},
            State#state{cb_state = UpdatedCbState, status = idle}
        %
        % {error, _} ->
        %
    end.

%%

handle_msg({system, From, Request}, Debug, State) ->
    sys:handle_system_msg(Request, From, State#state.parent, ?MODULE, Debug, State);
handle_msg(Msg, Debug, State) ->
    UpdatedDebug = sys:handle_debug(Debug, fun ?MODULE:write_debug/3, ?MODULE, {in, Msg}),
    UpdatedState = handle_non_system_msg(Msg, State),
    loop(UpdatedDebug, UpdatedState).

-spec handle_non_system_msg(term(), state()) -> state() | no_return().
handle_non_system_msg({Tag, Result}, #state{status = {waiting, Tag}} = State) ->
    handle_match_result(Result, State);
handle_non_system_msg(Msg, _State) ->
    terminate({unexpected_msg, Msg}).

-spec terminate(term()) -> no_return().
terminate(Reason) ->
    exit(Reason).
