-module(cbroker_pool).

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
    get_broker/0
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

%% ------------------------------------------------------------------
%% Record and Type Definitions
%% ------------------------------------------------------------------

-record(state, {
    settings :: settings(),
    broker :: reference(),
    workers :: #{pid() => worker()}
}).
-type state() :: #state{}.

-record(settings, {
    pool_size :: non_neg_integer()
}).
-type settings() :: #settings{}.

-record(worker, {
    pid :: pid(),
    start_ts :: timestamp(),
    ready_ts :: none | timestamp()
}).
-type worker() :: #worker{}.

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

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-spec init([]) -> {ok, state()}.
init([]) ->
    _ = process_flag(trap_exit, true),

    Settings = #settings{pool_size = 16},

    Broker = cbroker_nif:new(),
    persistent_term:put({ahhh, ?SERVER}, Broker),

    State = #state{
        settings = Settings,
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
    #settings{pool_size = PoolSize} = Settings, #state{workers = Workers} = State
) when
    map_size(Workers) < PoolSize
->
    UpdatedState = start_worker(State),
    start_missing_workers(Settings, UpdatedState);
start_missing_workers(#settings{}, #state{} = State) ->
    State.

start_worker(State) ->
    logger:notice("Expanding pool to ~p", [map_size(State#state.workers) + 1]),

    Args = #{
        broker => State#state.broker,
        cb => cbroker_testworker1,
        cb_args => [todo]
    },
    {ok, Pid} = cbroker_worker:start_link(Args),

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
