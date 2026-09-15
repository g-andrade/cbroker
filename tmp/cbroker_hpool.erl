-module(cbroker_hpool).

-ifdef(E48).
-moduledoc false.
-endif.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    child_spec/4,
    start_link/4,
    request/2,
    request/3
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(MOD_TYPE, handler).

-define(DEFAULT_REQ_TIMEOUT, (5_000)).

%% ------------------------------------------------------------------
%% API Type Definitions
%% ------------------------------------------------------------------

-type opts() :: [opt()].
-export_type([opts/0]).

%% ------------------------------------------------------------------
%% Internal Record and Type Definitions
%% ------------------------------------------------------------------

-type opt() ::
    ({size, non_neg_integer()}).
-export_type([opt/0]).

%% ------------------------------------------------------------------
%% Internal Record and Type Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec child_spec(Name, Module, Args, Opts) -> ChildSpec when
    Name :: LocalName | {local, LocalName} | {global, GlobalName} | {via, RegMod, ViaName},
    Module :: module(),
    Args :: term(),
    Opts :: opts(),
    LocalName :: atom(),
    GlobalName :: term(),
    RegMod :: module(),
    ViaName :: term(),
    ChildSpec :: supervisor:child_spec().

child_spec(Name, Module, Args, Opts) ->
    cbroker_pool:child_spec(Name, ?MOD_TYPE, Module, Args, Opts).

%%

-spec start_link(RegName, Module, Args, Opts) -> {ok, pid()} | {error, term()} when
    RegName :: LocalName | {local, LocalName} | {global, GlobalName} | {via, RegMod, ViaName},
    Module :: module(),
    Args :: term(),
    Opts :: opts(),
    LocalName :: atom(),
    GlobalName :: term(),
    RegMod :: module(),
    ViaName :: term().

start_link(Name, Module, Args, Opts) ->
    cbroker_pool:start_link(Name, ?MOD_TYPE, Module, Args, Opts).

%%

request(Name, Request) ->
    request(Name, Request, ?DEFAULT_REQ_TIMEOUT).

request(Name, Request, Timeout) ->
    {ok, Broker} = cbroker_pool:get_broker(Name, ?MOD_TYPE),
    ExchangeValue = {self(), Request},
    Deadline = timeout_deadline(Timeout),
    request_recur(Broker, ExchangeValue, Deadline).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

request_recur(Broker, ExchangeValue, Deadline) ->
    case cbroker_nif:ask(Broker, left, ExchangeValue, false) of
        {await, Tag} ->
            request_await_match(Tag, Deadline);
        %
        retry ->
            request_recur(Broker, ExchangeValue, Deadline);
        %
        Result ->
            request_handle_match_result(Result, Deadline)
    end.

request_await_match(Tag, Deadline) ->
    Timeout = millis_left_to_deadline(Deadline),

    receive
        {Tag, Result} ->
            request_handle_match_result(Result, Deadline)
    after Timeout ->
        request_await_match_timeout(Tag, Deadline)
    end.

request_await_match_timeout(Tag, Deadline) ->
    case cbroker_nif:cancel(Tag) of
        cancelled ->
            error(timeout);
        %
        too_late ->
            receive
                {Tag, Result} ->
                    request_handle_match_result(Result, Deadline)
            end
    end.

request_handle_match_result({match, MatchRef, WorkerPid}, Deadline) ->
    WorkerMon = monitor(process, WorkerPid),
    request_await_reply(MatchRef, WorkerPid, WorkerMon, Deadline).

request_await_reply(MatchRef, _WorkerPid, WorkerMon, Deadline) ->
    Timeout = millis_left_to_deadline(Deadline),

    receive
        {MatchRef, Reply} ->
            % TODO streaming
            demonitor(WorkerMon, [flush]),
            Reply
    after Timeout ->
        %cbroker_handler:cancel(WorkerPid, WorkerMon),
        % FIXME
        error(timeout)
    end.

%%%%%%%%%%%%

timeout_deadline(infinity) ->
    none;
timeout_deadline(Timeout) ->
    timestamp_now() + erlang:convert_time_unit(Timeout, millisecond, native).

millis_left_to_deadline(Deadline) ->
    TimeLeft = Deadline - timestamp_now(),
    ceil(TimeLeft / erlang:convert_time_unit(1, millisecond, native)).

timestamp_now() ->
    erlang:monotonic_time(native).
