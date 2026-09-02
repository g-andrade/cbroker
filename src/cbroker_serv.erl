-module(cbroker_serv).

-ifdef(E48).
-moduledoc false.
-endif.

-behaviour(gen_server).

-include("src/cbroker_shared_state.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    child_spec/1,
    start_link/1,
    get_shared_state/1
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

-define(SHARED_STATE_KEY(Name), ['___$cbroker.shared_state' | Name]).

%% ------------------------------------------------------------------
%% Record and Type Definitions
%% ------------------------------------------------------------------

-record(state, {
    shared_state_key :: term()
}).
-type state() :: #state{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec child_spec(atom()) -> supervisor:child_spec().
child_spec(Name) ->
    #{
        id => {?MODULE, Name},
        start => {?MODULE, start_link, [Name]}
    }.

-spec start_link(atom()) -> {ok, pid()} | {error, term()}.
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

-spec get_shared_state(atom()) -> #shared_state{} | none.
get_shared_state(Name) ->
    Key = ?SHARED_STATE_KEY(Name),

    try 
        persistent_term:get(Key)
    catch
        error:badarg ->
            none
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%-spec init([]) -> {ok, state()}.
init([Name]) ->
    _ = process_flag(trap_exit, true), % almost always call `terminate/2`
    SharedStateKey = ?SHARED_STATE_KEY(Name),
    SharedState = new_shared_state(),
    persistent_term:put(SharedStateKey, SharedState),
    {ok, #state{shared_state_key = SharedStateKey}}.

-spec handle_call(Request, From, State) -> {stop, Reason, State} when
    Request :: term(),
    From :: gen_server:from(),
    State :: state(),
    Reason :: {unexpected_call, #{request := term(), from := gen_server:from()}}.
handle_call(Request, From, State) ->
    ErrorDetails = #{request => Request, from => From},
    {stop, {unexpected_call, ErrorDetails}, State}.

-spec handle_cast(Request, State) -> {stop, {unexpected_cast, term()}, State} when
    Request :: term(),
    State :: state().
handle_cast(Request, State) ->
    {stop, {unexpected_cast, Request}, State}.

-spec handle_info(Info, State) -> {stop, {unexpected_info, term()}, State} when
    Info :: term(),
    State :: state().
handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

-spec terminate(term(), state()) -> ok.
terminate(Reason, #state{shared_state_key =  SharedStateKey}) ->
    _ = (is_termination_reason_healthy(Reason)
         andalso persistent_term:erase(SharedStateKey)),
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

new_shared_state() ->
    #shared_state{ 
       broker = cbroker_nif:new(),
       instance = erlang:unique_integer()
      }.

is_termination_reason_healthy(normal) -> true;
is_termination_reason_healthy(shutdown) -> true;
is_termination_reason_healthy({shutdown, _}) -> true;
is_termination_reason_healthy(_) -> false.
