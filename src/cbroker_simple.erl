-module(cbroker_simple).

-ifdef(E48).
-moduledoc false.
-endif.

-include_lib("stdlib/include/assert.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    child_spec/0,
    start_link/0,
    async_ask/3,
    ask/3
]).

-ignore_xref([start_link/0]).

%% ------------------------------------------------------------------
%% sys Function Exports
%% ------------------------------------------------------------------

-export([
    init/1,
    system_code_change/4,
    system_continue/3,
    system_terminate/4,
    write_debug/3
]).

-ignore_xref([
    init/1,
    system_code_change/4,
    system_continue/3,
    system_terminate/4,
    write_debug/3
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(SERVER, ?MODULE).

-define(ENTRY(Pid, Value, Tag, Mon), {Pid, Value, Tag, Mon}).

%% ------------------------------------------------------------------
%% Record and Type Definitions
%% ------------------------------------------------------------------

-record(state, {
    invariants :: invariants(),
    side :: none | left | right,
    q :: queue:queue(entry()),
    downed_mons :: downed_mons()
}).
-type state() :: #state{}.

-type entry() :: ?ENTRY(pid(), term(), reference(), reference()).

-type downed_mons() :: #{reference() => v}.

%%

-record(invariants, {
    parent :: pid()
}).
-type invariants() :: #invariants{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?SERVER,
        start => {?MODULE, start_link, []}
    }.

-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

async_ask(Side, Pid, Value) ->
    ServPid = whereis(?SERVER),
    Tag = monitor(process, ServPid),
    _ = ServPid ! {ask, Side, Pid, Value, Tag},
    {await, ServPid, Tag}.

ask(Side, Pid, Value) ->
    {await, _, Tag} = async_ask(Side, Pid, Value),

    receive
        {T, Reply} when T =:= Tag ->
            Reply;
        %
        {'DOWN', Ref, _, _, Reason} when Ref =:= Tag ->
            exit({queue_down, Reason})
    end.

%% ------------------------------------------------------------------
%% sys Function Definitions
%% ------------------------------------------------------------------

-spec init(pid()) -> no_return().
init(Parent) ->
    try register(?SERVER, self()) of
        true ->
            Debug = sys:debug_options([]),

            Invariants = #invariants{parent = Parent},

            State = #state{
                invariants = Invariants,
                side = none,
                q = queue:new(),
                downed_mons = #{}
            },

            proc_lib:init_ack(Parent, {ok, self()}),
            loop(Debug, State)
    catch
        error:badarg ->
            ExistingPid = whereis(?SERVER),
            proc_lib:init_ack(Parent, {error, {already_started, ExistingPid}}),
            exit(normal)
    end.

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

loop(Debug, State) ->
    receive
        Msg ->
            handle_msg(Msg, Debug, State)
    end.

handle_msg({system, From, Request}, Debug, State) ->
    Parent = state_parent(State),
    sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
handle_msg(Msg, Debug, State) ->
    UpdatedDebug = sys:handle_debug(Debug, fun ?MODULE:write_debug/3, ?MODULE, {in, Msg}),
    UpdatedState = handle_non_system_msg(Msg, State),
    loop(UpdatedDebug, UpdatedState).

-spec handle_non_system_msg(term(), state()) -> state() | no_return().
handle_non_system_msg({ask, Side, Pid, Value, Tag}, State) ->
    handle_ask(Side, Pid, Value, Tag, State);
handle_non_system_msg({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    handle_monitor_down(Ref, State);
handle_non_system_msg(Msg, _State) ->
    terminate({unexpected_msg, Msg}).

-spec terminate(term()) -> no_return().
terminate(Reason) ->
    exit(Reason).

%%

handle_ask(Side, Pid, Value, Tag, #state{side = QSide, q = Q, downed_mons = DownedMons} = State) ->
    case Side =:= QSide of
        true ->
            Mon = monitor(process, Pid),
            Entry = ?ENTRY(Pid, Value, Tag, Mon),
            UpdatedQ = queue:in(Entry, Q),
            State#state{q = UpdatedQ};
        %
        _ when QSide =:= none ->
            Mon = monitor(process, Pid),
            Entry = ?ENTRY(Pid, Value, Tag, Mon),
            UpdatedQ = queue:in(Entry, Q),
            State#state{side = Side, q = UpdatedQ};
        %
        _ ->
            case map_size(DownedMons) of
                0 ->
                    try_matching1(Side, Pid, Value, Tag, Q, State);
                %
                _ ->
                    try_matching2(Side, Pid, Value, Tag, Q, DownedMons, State)
            end
    end.

try_matching1(Side2, Pid2, Value2, Tag2, Q, State) ->
    case queue:out(Q) of
        {{value, ?ENTRY(Pid1, Value1, Tag1, Mon1)}, RemainingQ} ->
            match(Pid1, Value1, Tag1, Mon1, Pid2, Value2, Tag2),

            case queue:is_empty(RemainingQ) of
                false ->
                    State#state{q = RemainingQ};
                %
                true ->
                    State#state{side = none, q = RemainingQ}
            end;
        %
        {empty, EmptyQ} ->
            Mon = monitor(process, Pid2),
            Entry = ?ENTRY(Pid2, Value2, Tag2, Mon),
            UpdatedQ = queue:in(Entry, EmptyQ),
            State#state{side = Side2, q = UpdatedQ, downed_mons = #{}}
    end.

try_matching2(Side2, Pid2, Value2, Tag2, Q, DownedMons, State) ->
    case queue:out(Q) of
        {{value, ?ENTRY(Pid1, Value1, Tag1, Mon1)}, RemainingQ} ->
            case maps:take(Mon1, DownedMons) of
                error ->
                    match(Pid1, Value1, Tag1, Mon1, Pid2, Value2, Tag2),

                    case queue:is_empty(RemainingQ) of
                        false ->
                            State#state{q = RemainingQ, downed_mons = DownedMons};
                        %
                        true ->
                            State#state{side = none, q = RemainingQ, downed_mons = #{}}
                    end;
                %
                {_, RemainingMons} ->
                    case map_size(RemainingMons) of
                        0 ->
                            try_matching2(
                                Side2, Pid2, Value2, Tag2, RemainingQ, RemainingMons, State
                            );
                        %
                        _ ->
                            try_matching1(Side2, Pid2, Value2, Tag2, RemainingQ, State)
                    end
            end;
        %
        {empty, EmptyQ} ->
            Mon = monitor(process, Pid2),
            Entry = ?ENTRY(Pid2, Value2, Tag2, Mon),
            UpdatedQ = queue:in(Entry, EmptyQ),
            State#state{side = Side2, q = UpdatedQ, downed_mons = #{}}
    end.

match(Pid1, Value1, Tag1, Mon1, Pid2, Value2, Tag2) ->
    demonitor(Mon1),
    MatchRef = Mon1,
    _ = Pid1 ! {Tag1, {match, MatchRef, Value2}},
    _ = Pid2 ! {Tag2, {match, MatchRef, Value1}},
    ok.

%%

handle_monitor_down(Ref, #state{downed_mons = Mons} = State) ->
    UpdatedMons = Mons#{Ref => v},

    case map_size(UpdatedMons) >= 10 of
        false ->
            State#state{downed_mons = UpdatedMons};
        %
        true ->
            FilterFun = fun(?ENTRY(_, _, _, Mon)) -> not maps:is_key(Mon, UpdatedMons) end,
            FilteredQ = queue:filter(FilterFun, State#state.q),

            case queue:is_empty(FilteredQ) of
                true ->
                    State#state{side = none, q = FilteredQ, downed_mons = #{}};
                %
                false ->
                    State#state{q = FilteredQ, downed_mons = #{}}
            end
    end.
