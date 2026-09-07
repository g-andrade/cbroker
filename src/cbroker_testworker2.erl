-module(cbroker_testworker2).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of this module.".
-endif.

-behaviour(cbroker_handler).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_request/3, handle_requester_down/3, handle_info/2]).

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

handle_request({sleep_between, Min, Max}, From, State) ->
    NapTime = Min + rand:uniform(Max - Min) - 1,

    case rand:uniform(3) of
        1 ->
            logger:notice("DIRECT REPLY"),
            timer:sleep(NapTime),
            {reply, reply(), State};
        %
        2 ->
            logger:notice("ASYNC REPLY"),
            _ = self() ! {nap_async_and_reply, From, NapTime},
            {reply_later, State};
        %
        3 ->
            logger:notice("SLOT TAKE REPLY"),
            _ = self() ! {nap_async_and_return, From, NapTime},
            {slot_take, State}
    end.

handle_requester_down(_From, _Reason, State) ->
    case rand:uniform(2) of
        1 ->
            {cancelled, State};
        %
        2 ->
            _ = self() ! cancel,
            {cancelling, State}
    end.

handle_info({nap_async_and_reply, From, NapTime}, State) ->
    timer:sleep(NapTime),
    {reply, From, reply(), State};
handle_info({nap_async_and_return, From, NapTime}, State) ->
    timer:sleep(NapTime),
    {Pid, Tag} = From,
    _ = Pid ! {Tag, reply()},
    {slot_return, State};
handle_info(cancel, State) ->
    {cancelled, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

reply() -> {alright, self()}.
