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

-module(cbroker).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of the `cbroker` public API.".
-endif.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([]).

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

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
