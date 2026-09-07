-module(cbroker_wpool).

-ifdef(E48).
-moduledoc false.
-endif.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    child_spec/4,
    start_link/4,
    %
    checkout/1,
    checkout/2,
    checkout/3,
    %
    checkin/1,
    %
    transaction/2,
    transaction/3
]).

-ignore_xref([start_link/0]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(MOD_TYPE, worker).

-define(DEFAULT_CHECKOUT_TIMEOUT, (5_000)).
-define(DEFAULT_TRANSACTION_TIMEOUT, (5_000)).

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

checkout(Name) ->
    checkout(Name, true).

checkout(Name, Block) ->
    checkout(Name, Block, ?DEFAULT_CHECKOUT_TIMEOUT).

checkout(Name, Block, Timeout) ->
    {ok, Broker} = cbroker_pool:get_broker(Name, ?MOD_TYPE),

    case Block of
        true ->
            Deadline = timeout_deadline(Timeout),
            blocking_checkout_recur(Broker, Deadline);
        %
        false ->
            nb_checkout(Broker)
    end.

checkin(Handle) ->
    WrapperPid = Handle,
    cbroker_worker:checkin(WrapperPid).

transaction(Name, Fun) ->
    transaction(Name, Fun, ?DEFAULT_TRANSACTION_TIMEOUT).

transaction(Name, Fun, Timeout) ->
    case checkout(Name, true, Timeout) of
        {ok, Handle, WorkerPid} ->
            try
                Fun(WorkerPid)
            after
                checkin(Handle)
            end
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% blocking check-out

blocking_checkout_recur(Broker, Deadline) ->
    ExchangeValue = {checkout, self()},

    case cbroker_nif:ask(Broker, left, ExchangeValue, false) of
        {await, Tag} ->
            blocking_checkout_await(Broker, Tag, Deadline);
        %
        {match, _MatchRef, {WrapperPid, WorkerPid}} ->
            Handle = WrapperPid,
            {ok, Handle, WorkerPid};
        %
        retry ->
            blocking_checkout_recur(Broker, Deadline)
    end.

blocking_checkout_await(Broker, Tag, Deadline) ->
    Timeout = millis_left_to_deadline(Deadline),

    receive
        {Tag, Result} ->
            {match, _MatchRef, {WrapperPid, WorkerPid}} = Result,
            Handle = WrapperPid,
            {ok, Handle, WorkerPid}
    after Timeout ->
        blocking_checkout_timeout(Broker, Tag)
    end.

blocking_checkout_timeout(Broker, Tag) ->
    case cbroker_nif:cancel(Broker, Tag) of
        cancelled ->
            error(timeout);
        %
        too_late ->
            receive
                {Tag, Result} ->
                    blocking_checkout_timeout_concurrent_result(Result)
            end
    end.

blocking_checkout_timeout_concurrent_result(Result) ->
    case Result of
        {match, _MatchRef, {WrapperPid, WorkerPid}} ->
            Handle = WrapperPid,
            {ok, Handle, WorkerPid}
    end.

%%%%%%%%%%%%
%% non-blocking check-out

nb_checkout(Broker) ->
    ExchangeValue = {checkout, self()},

    case cbroker_nif:ask(Broker, left, ExchangeValue, false, nb) of
        {await, Tag} ->
            nb_checkout_await(Tag);
        %
        {match, _, {WrapperPid, WorkerPid}} ->
            Handle = WrapperPid,
            {ok, Handle, WorkerPid};
        %
        retry ->
            full;
        %
        cancelled ->
            full
    end.

nb_checkout_await(Tag) ->
    receive
        {Tag, Result} ->
            case Result of
                {match, _, WorkerPid} ->
                    WorkerPid
            end
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
