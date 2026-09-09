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

-module(cbroker_sup).

-ifdef(E48).
-moduledoc false.
-endif.

-behaviour(supervisor).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% supervisor Function Exports
%% ------------------------------------------------------------------

-export([init/1]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% ------------------------------------------------------------------
%% supervisor Function Definitions
%% ------------------------------------------------------------------

-if(?OTP_RELEASE >= 24).
-dialyzer({no_underspecs, init/1}).
-else.
-dialyzer({nowarn_function, init/1}).
-endif.

-spec init(InitArgs) -> {ok, {SupFlags, ChildSpecs}} when
    InitArgs :: [],
    SupFlags :: supervisor:sup_flags(),
    ChildSpecs :: [supervisor:child_spec()].
init([]) ->
    SupFlags = sup_flags(),
    ChildSpecs = child_specs(),
    {ok, {SupFlags, ChildSpecs}}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

sup_flags() ->
    #{
        strategy => one_for_one,
        intensity => 5,
        period => 1
    }.

child_specs() ->
    [
        cbroker_simple:child_spec(),
        cbroker_serv:child_spec(test)
        %cbroker_hpool:child_spec(magic2, cbroker_testworker2, [todo], [{size, 8}]),
        %cbroker_wpool:child_spec(magic3, cbroker_testworker3, [todo], [{size, 8}])
    ].
