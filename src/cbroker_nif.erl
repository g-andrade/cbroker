%% @moduledoc false
-module(cbroker_nif).

-on_load(init/0).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    alloc_counters/0,
    ask/5,
    cancel/1,
    debug_info/1,
    new/1
]).

-ignore_xref([
    alloc_counters/0
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(APPNAME, cbroker).
-define(LIBNAME, cbroker).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

ask(BrokerRef, Lane, Offer, ReplyRef, AskType) ->
    OfferSizeArg = offer_size_arg(Offer),
    ask(BrokerRef, Lane, Offer, OfferSizeArg, ReplyRef, AskType).

cancel(_Ticket) ->
    not_loaded(?LINE).

debug_info(_BrokerRef) ->
    not_loaded(?LINE).

new(_Opts) ->
    not_loaded(?LINE).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

init() ->
    SoName =
        case code:priv_dir(?APPNAME) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, ?LIBNAME]);
                    _ ->
                        filename:join([priv, ?LIBNAME])
                end;
            Dir ->
                filename:join(Dir, ?LIBNAME)
        end,
    erlang:load_nif(SoName, 0).

ask(_BrokerRef, _Lane, _Offer, _OfferSizeArg, _ReplyRef, _AskType) ->
    not_loaded(?LINE).

% Live allocations across every broker, for the tests to assert nothing leaked
alloc_counters() ->
    not_loaded(?LINE).

-if(?OTP_RELEASE < 29).
offer_size_arg(Value) ->
    erts_debug:flat_size(Value).
-else.
offer_size_arg(_Value) ->
    compute_from_nif.
-endif.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
