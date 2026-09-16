-module(cbroker_persistent).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of this module.".
-endif.

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    child_spec/2,
    start_link/2,
    get/1
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([
    init/1,
    terminate/2,
    code_change/3
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(PERSISTENT_KEY(LookupName), ['___$cbroker_persistent' | LookupName]).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

-type reg_name() :: {local, atom()} | {global, term()} | {via, module(), term()}.
-export_type([reg_name/0]).

-type lookup_name() :: atom() | {global, term()} | {via, module(), term()}.
-export_type([lookup_name/0]).

-record(state, {key :: term()}).
-type state() :: #state{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec child_spec(Name, Opts) -> supervisor:child_spec() when
    Name :: reg_name(),
    Opts :: cbroker:broker_opts().
child_spec(Name, Opts) ->
    #{
        id => {?MODULE, Name},
        start => {?MODULE, start_link, [Name, Opts]}
    }.

-spec start_link(Name, Opts) -> {ok, pid()} | {error, term()} when
    Name :: reg_name(),
    Opts :: cbroker:broker_opts().
start_link(Name, Opts) ->
    gen_server:start_link(Name, ?MODULE, [Name, Opts], []).

-spec get(reg_name() | lookup_name()) -> cbroker:broker_ref() | no_return().
get(Name) ->
    LookupName = lookup_name(Name),
    Key = ?PERSISTENT_KEY(LookupName),

    try
        persistent_term:get(Key)
    catch
        error:badarg ->
            error({broker_not_found, LookupName})
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-spec init([InitArg, ...]) -> {ok, state()} when
    InitArg :: Name | Opts,
    Name :: reg_name(),
    Opts :: cbroker:broker_opts().
init([Name, Opts]) ->
    % always call `terminate/2` unless killed
    _ = process_flag(trap_exit, true),
    LookupName = lookup_name(Name),
    Key = ?PERSISTENT_KEY(LookupName),
    MergedOpts = [depends_on_creator | proplists:delete(depends_on_creator, Opts)],
    Broker = cbroker:new(MergedOpts),
    persistent_term:put(Key, Broker),
    {ok, #state{key = Key}}.

-spec terminate(term(), state()) -> ok.
terminate(Reason, State) ->
    _ =
        (cbroker_utils:is_termination_reason_healthy(Reason) andalso
            persistent_term:erase(State#state.key)),
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

lookup_name(LookupName) when is_atom(LookupName) ->
    LookupName;
lookup_name({local, LookupName}) when is_atom(LookupName) ->
    LookupName;
lookup_name({global, _} = LookupName) ->
    LookupName;
lookup_name({via, Module, _} = LookupName) when is_atom(Module) ->
    LookupName;
lookup_name(InvalidName) ->
    error({invalid_name, InvalidName}).
