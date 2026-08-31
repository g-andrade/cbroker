%% @copyright 2026 Guilherme Andrade
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy  of this software and associated documentation files (the "Software"),
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

-module(cbroker2).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of the `cbroker` public API.".
-endif.

-include("src/cbroker_shared_state.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    ask/1,
    ask/2,
    ask/3,
    ask_r/1,
    ask_r/2,
    ask_r/3,
    %
    async_ask/1,
    async_ask/2,
    async_ask_r/1,
    async_ask_r/2,
    %
    ask_side/4,
    await_after_ask/2,
    async_ask_side/3

]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(DEFAULT_TIMEOUT, 5_000).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

% Documented public API functions follow the pattern below. Doc attributes are
% guarded by `-ifdef(E48)` so the source still compiles on OTP < 27, which lacks
% EEP-48 `-doc`/`-moduledoc`. Hide internals with `-doc false` / `-moduledoc
% false` (NOT `@private`, which ex_doc ignores). A public function that isn't
% called internally needs `-ignore_xref/1` to satisfy the `exports_not_used`
% xref check.
%
%     -export([add/2]).
%     -ignore_xref([add/2]).
%
%     -ifdef(E48).
%     -doc "Adds two integers.".
%     -endif.
%     -spec add(integer(), integer()) -> integer().
%     add(A, B) ->
%         A + B.

ask(Name) ->
    ask(Name, self()).

ask(Name, Value) ->
    ask(Name, Value, ?DEFAULT_TIMEOUT).

ask(Name, Value, Timeout) ->
    ask_side(Name, left, Value, Timeout).

ask_r(Name) ->
    ask_r(Name, self()).

ask_r(Name, Value) ->
    ask_r(Name, Value, ?DEFAULT_TIMEOUT).

ask_r(Name, Value, Timeout) ->
    ask_side(Name, right, Value, Timeout).

%%

async_ask(Name) ->
    async_ask(Name, self()).

async_ask(Name, Value) ->
    async_ask_side(Name, left, Value).

async_ask_r(Name) ->
    async_ask_r(Name, self()).

async_ask_r(Name, Value) ->
    async_ask_side(Name, right, Value).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

ask_side(Name, Side, Value, Timeout) ->
    case cbroker_serv:get_shared_state(Name) of
        #shared_state{broker2 = Broker} ->
            %
            case cbroker_nif2:ask(Broker, Side, Value) of
                {await, Ticket} ->
                    await_after_ask(Ticket, Timeout);
                %
                {match, _} = Match ->
                    Match
            end;
        %
        none ->
            not_running
    end.

await_after_ask(Ticket, Timeout) ->
    receive
        {T, Result} when T =:= Ticket ->
            Result
    after
        Timeout ->
            case cbroker_nif2:cancel(Ticket) of
                cancelled ->
                    timeout;
                %
                too_late ->
                    receive
                        {T, Result} when T =:= Ticket ->
                            Result
                    end
            end
    end.

async_ask_side(Name, Side, Value) ->
    case cbroker_serv:get_shared_state(Name) of
        #shared_state{broker2 = Broker} ->
            case cbroker_nif2:ask(Broker, Side, Value) of
                {await, _} = Await ->
                    Await;
                %
                {match, Match} ->
                    % FIXME
                    FauxTicket = make_ref(),
                    _ = self() ! {match, FauxTicket, Match},
                    {await, FauxTicket};
                %
                retry ->
                    async_ask_side(Name, Side, Value)

            end;
        %
        none ->
            not_running
    end.
