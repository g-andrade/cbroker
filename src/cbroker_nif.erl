%% Copyright (c) 2026 Guilherme Andrade
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the "Software"),
%% to deal in the Software without restriction, including without limitation
%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%% and/or sell copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
%% DEALINGS IN THE SOFTWARE.

-module(cbroker_nif).

-ifdef(E48).
-moduledoc false.
-endif.

-on_load(init/0).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-ignore_xref([
    alloc_perfcounters/0
]).

-export([
    alloc_perfcounters/0,
    ask/5,
    cancel/1,
    debug_info/1,
    new/1
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
%% Static Check Tweaks
%% ------------------------------------------------------------------

-if(?OTP_RELEASE >= 29).
-hank([{unnecessary_function_arguments, [{offer_size_arg, 1, 1}]}]).
-endif.

-hank([
    {unnecessary_function_arguments, [
        ask,
        cancel,
        debug_info,
        new
    ]}
]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

% Live allocations across every broker, for the tests to assert nothing leaked
-spec alloc_perfcounters() -> list() | unavailable.
alloc_perfcounters() ->
    not_loaded(?LINE).

%%

-spec ask(BrokerRef, Lane, Offer, ReplyRef, AskType) ->
    {await, Ticket}
    | {match, MatchRef, CounterOffer, SojournTime}
    | {drop, DropReason, SojournTime}
    | {error, closed}
when
    BrokerRef :: reference(),
    Lane :: left | right,
    Offer :: term(),
    ReplyRef :: ticket | reference(),
    AskType :: dynamic | async | non_blocking,
    Ticket :: reference(),
    MatchRef :: reference(),
    CounterOffer :: term(),
    DropReason :: cbroker:drop_reason(),
    SojournTime :: non_neg_integer().

ask(BrokerRef, Lane, Offer, ReplyRef, AskType) ->
    OfferSizeArg = offer_size_arg(Offer),
    ask(BrokerRef, Lane, Offer, OfferSizeArg, ReplyRef, AskType).

%%

-spec cancel(Ticket) -> {cancelled, SojournTime} | too_late when
    Ticket :: reference(),
    SojournTime :: non_neg_integer().

cancel(_Ticket) ->
    not_loaded(?LINE).

%%

-spec debug_info(BrokerRef) -> DebugInfo when
    BrokerRef :: reference(),
    DebugInfo :: term().

debug_info(_BrokerRef) ->
    not_loaded(?LINE).

%%

-spec new([Opt]) -> BrokerRef when
    Opt :: term(),
    BrokerRef :: reference().

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

% The NIF can only size a term by itself from OTP 29 on (`enif_term_size`)
-if(?OTP_RELEASE < 29).
offer_size_arg(Value) ->
    erts_debug:flat_size(Value).
-else.
offer_size_arg(_Value) ->
    compute_from_nif.
-endif.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
