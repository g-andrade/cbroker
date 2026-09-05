-module(cbroker_testworker1).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of this module.".
-endif.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_work/3]).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

-record(state, {}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

init([todo]) ->
    timer:sleep(rand:uniform(100)),
    {ok, #state{}}.

handle_work({sleep_between, Min, Max}, _From, State) ->
    NapTime = Min + rand:uniform(Max - Min) - 1,
    timer:sleep(NapTime),
    {reply, {alright, self()}, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
