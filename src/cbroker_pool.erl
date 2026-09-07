-module(cbroker_pool).

-ifdef(E48).
-moduledoc false.
-endif.

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    child_spec/5,
    start_link/5,
    get_broker/2
]).

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

-define(SHARED_STATE_KEY(DispatchingName), ['__$cbroker_pool.shared_state' | DispatchingName]).

%% ------------------------------------------------------------------
%% API Type Definitions
%% ------------------------------------------------------------------

-type opts() :: [opt()].
-export_type([opts/0]).

%% ------------------------------------------------------------------
%% Internal Record and Type Definitions
%% ------------------------------------------------------------------

-type cb_type() :: worker | handler.
-export_type([cb_type/0]).

-type opt() ::
    ({size, non_neg_integer()}).
-export_type([opt/0]).

%% ------------------------------------------------------------------
%% Internal Record and Type Definitions
%% ------------------------------------------------------------------

-record(shared_state, {
    broker :: cbroker_nif:broker(),
    cb_type :: cb_type(),
    serv_pid :: pid()
}).

-record(state, {
    settings :: settings(),
    shared_state_key :: nonempty_improper_list(atom(), term()),
    broker :: cbroker_nif:broker(),
    workers :: #{pid() => worker()}
}).
-type state() :: #state{}.

-record(settings, {
    cb :: module(),
    cb_init_args :: term(),
    cb_type :: cb_type(),
    size :: non_neg_integer()
}).
-type settings() :: #settings{}.

-record(worker, {
    pid :: pid(),
    start_ts :: timestamp(),
    ready_ts :: none | timestamp()
}).
-type worker() :: #worker{}.

-type timestamp() :: integer().

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec child_spec(Name, ModType, Module, Args, Opts) -> ChildSpec when
    Name :: LocalName | {local, LocalName} | {global, GlobalName} | {via, RegMod, ViaName},
    ModType :: cb_type(),
    Module :: module(),
    Args :: term(),
    Opts :: opts(),
    LocalName :: atom(),
    GlobalName :: term(),
    RegMod :: module(),
    ViaName :: term(),
    ChildSpec :: supervisor:child_spec().

child_spec(Name, ModType, Module, Args, Opts) ->
    RegName = cbroker_utils:reg_name(Name),
    #{
        id => {?MODULE, RegName},
        start => {?MODULE, start_link, [RegName, ModType, Module, Args, Opts]},
        % key for release upgrades
        type => supervisor
    }.

%%

-spec start_link(RegName, ModType, Module, Args, Opts) -> {ok, pid()} | {error, term()} when
    RegName :: LocalName | {local, LocalName} | {global, GlobalName} | {via, RegMod, ViaName},
    ModType :: cb_type(),
    Module :: module(),
    Args :: term(),
    Opts :: opts(),
    LocalName :: atom(),
    GlobalName :: term(),
    RegMod :: module(),
    ViaName :: term().

start_link(Name, ModType, Module, Args, Opts) ->
    RegName = cbroker_utils:reg_name(Name),

    try new_settings(ModType, Module, Args, Opts) of
        Settings ->
            gen_server:start_link(RegName, ?MODULE, [RegName, Settings], [])
    catch
        {badopts, Reason} ->
            {error, {badopts, Reason}};
        %
        {badopt, Opt} ->
            {error, {badopt, Opt}}
    end.

get_broker(Name, ExpectedCbType) ->
    DispatchingName = cbroker_utils:dispatching_name(Name),
    SharedStateKey = ?SHARED_STATE_KEY(DispatchingName),

    try persistent_term:get(SharedStateKey) of
        #shared_state{cb_type = ExpectedCbType, broker = Broker} ->
            {ok, Broker};
        %
        #shared_state{cb_type = CbType} ->
            {unexpected_cb_type, CbType}
    catch
        error:badarg ->
            not_running
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-spec init([RegName | Settings, ...]) -> {ok, state()} when
    RegName :: {local, LocalName} | {global, GlobalName} | {via, RegMod, ViaName},
    Settings :: settings(),
    LocalName :: atom(),
    GlobalName :: term(),
    RegMod :: module(),
    ViaName :: term().

init([RegName, Settings]) ->
    _ = process_flag(trap_exit, true),

    DispatchingName = cbroker_utils:dispatching_name(RegName),
    SharedStateKey = ?SHARED_STATE_KEY(DispatchingName),
    Broker = cbroker_nif:new(),
    SharedState = #shared_state{
        broker = Broker,
        cb_type = Settings#settings.cb_type,
        serv_pid = self()
    },
    persistent_term:put(SharedStateKey, SharedState),

    State = #state{
        settings = Settings,
        shared_state_key = SharedStateKey,
        broker = Broker,
        workers = #{}
    },

    State2 = start_missing_workers(State),
    {ok, State2}.

%-spec handle_call(Request, From, State) -> {stop, Reason, State} when
%    Request :: term(),
%    From :: gen_server:from(),
%    State :: state(),
%    Reason :: {unexpected_call, #{request := term(), from := gen_server:from()}}.
handle_call(which_chidlren, _From, State) ->
    handle_which_children_call(State);
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
handle_info({'EXIT', Pid, Reason}, State) ->
    handle_linked_process_exit(Pid, Reason, State);
handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

-spec terminate(term(), state()) -> ok.
terminate(Reason, #state{shared_state_key = SharedStateKey}) ->
    _ =
        (cbroker_utils:is_termination_reason_wholesome(Reason) andalso
            persistent_term:erase(SharedStateKey)),
    ok.

-spec code_change(term(), state() | term(), term()) ->
    {ok, state()} | {error, {cannot_convert_state, term()}}.
code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State};
code_change(_OldVsn, State, _Extra) ->
    {error, {cannot_convert_state, State}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions: Server
%% ------------------------------------------------------------------

new_settings(ModType, Module, Args, Opts) ->
    case infer_cb_type(Module) of
        {ok, CbType} when CbType =:= ModType ->
            Default = #settings{
                cb = Module,
                cb_init_args = Args,
                cb_type = CbType,
                size = erlang:system_info(schedulers)
            },
            new_settings_recur(Opts, Default);
        %
        {ok, worker} ->
            {error,
                {module, Module,
                    'Module declares behaviour cbroker_worker but pool is configured to cbroker_handler'}};
        %
        {ok, handler} ->
            {error,
                {module, Module,
                    'Module declares behaviour cbroker_handler but pool is configured to cbroker_worker'}};
        %
        {error, Reason} ->
            {error, {module, Module, Reason}}
    end.

infer_cb_type(Module) ->
    Behaviours = module_behaviours(Module),
    HasWorker = lists:member(cbroker_worker, Behaviours),
    HasHandle = lists:member(cbroker_handler, Behaviours),

    case {HasWorker, HasHandle} of
        {true, true} ->
            {error, 'Both cbroker_worker and cbroker_handler behaviours were declared'};
        %
        {true, false} ->
            {ok, worker};
        %
        {false, true} ->
            {ok, handler};
        %
        {false, false} ->
            {error, 'Neither cbroker_worker nor cbroker_handler behaviour was declared'}
    end.

module_behaviours(Module) ->
    Attributes = Module:module_info(attributes),
    lists:usort(
        lists:flatten([
            proplists:get_all_values(behavior, Attributes),
            proplists:get_all_values(behaviour, Attributes)
        ])
    ).

new_settings_recur([Opt | Next], Acc) ->
    UpdatedAcc = new_setting(Opt, Acc),
    new_settings_recur(Next, UpdatedAcc);
new_settings_recur([], Acc) ->
    Acc;
new_settings_recur(Opts, _) ->
    throw({badopts, Opts}).

new_setting({size, Size}, Acc) when is_integer(Size), Size >= 0 ->
    Acc#settings{size = Size};
new_setting(Opt, _) ->
    throw({badopt, Opt}).

start_missing_workers(#state{settings = Settings} = State) ->
    start_missing_workers(Settings, State).

start_missing_workers(
    #settings{size = PoolSize} = Settings, #state{workers = Workers} = State
) when
    map_size(Workers) < PoolSize
->
    UpdatedState = start_worker(State),
    start_missing_workers(Settings, UpdatedState);
start_missing_workers(#settings{}, #state{} = State) ->
    State.

start_worker(#state{settings = Settings} = State) ->
    logger:notice("Expanding pool to ~p", [map_size(State#state.workers) + 1]),

    Wrapper = worker_wrapper(Settings#settings.cb_type),
    Args = #{
        broker => State#state.broker,
        cb => Settings#settings.cb,
        cb_args => Settings#settings.cb_init_args
    },
    {ok, Pid} = Wrapper:start_link(Args),

    Worker = #worker{
        pid = Pid,
        start_ts = timestamp_now(),
        ready_ts = none
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

%%%

handle_linked_process_exit(Pid, _Reason, #state{workers = Workers} = State) ->
    case maps:take(Pid, Workers) of
        {#worker{} = Worker, Remaining} ->
            handle_worker_exit(Pid, Worker, Remaining, State);
        %
        error ->
            {noreply, State}
    end.

handle_worker_exit(_Pid, _Worker, Remaining, #state{} = State) ->
    State2 = State#state{workers = Remaining},
    State3 = start_missing_workers(State2),
    {noreply, State3}.

timestamp_now() ->
    erlang:monotonic_time(native).

%% ------------------------------------------------------------------
%% Internal Function Definitions: Release Upgrades
%% ------------------------------------------------------------------

% -spec which_children(SupRef) -> [{Id, Child, Type, Modules}]
%                         when
%                             SupRef :: sup_ref(),
%                             Id :: child_id() | undefined,
%                             Child :: child() | restarting,
%                             Type :: worker(),
%                             Modules :: modules().

handle_which_children_call(#state{settings = Settings, workers = Workers} = State) ->
    WorkerWrapper = worker_wrapper(Settings#settings.cb_type),
    % FIXME
    WorkerModules = [WorkerWrapper, Settings#settings.cb],
    Reply = [which_children_element(Worker, WorkerModules) || Worker <- maps:values(Workers)],
    {reply, Reply, State}.

worker_wrapper(worker) -> cbroker_worker;
worker_wrapper(handler) -> cbroker_handler.

which_children_element(#worker{pid = Pid}, Modules) ->
    Id = {worker, Pid},
    Child = Pid,
    Type = worker,
    {Id, Child, Type, Modules}.
