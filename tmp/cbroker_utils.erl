-module(cbroker_utils).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of this module.".
-endif.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    reg_name/1,
    dispatching_name/1,
    is_termination_reason_wholesome/1
]).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec reg_name(atom() | Other) -> {local, atom()} | Other.
reg_name(Local) when is_atom(Local) ->
    {local, Local};
reg_name(Name) ->
    Name.

-spec dispatching_name({local, atom()} | Other) -> atom() | Other.
dispatching_name({local, Local}) when is_atom(Local) ->
    Local;
dispatching_name(Name) ->
    Name.

is_termination_reason_wholesome(normal) -> true;
is_termination_reason_wholesome(shutdown) -> true;
is_termination_reason_wholesome({shutdown, _}) -> true;
is_termination_reason_wholesome(_) -> false.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
