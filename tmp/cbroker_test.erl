-module(cbroker_test).

-export(
    [
        test2_start/0,
        test2_set_rate/1,
        test2_stop/0
    ]
).

test2_start() ->
    spawn_link(fun run_test2/0).

test2_set_rate(Rate) ->
    Pid = whereis(test2),
    Pid ! {set_rate, Rate},
    ok.

test2_stop() ->
    Pid = whereis(test2),
    Pid ! stop,
    ok.

%%

%%

run_test2() ->
    register(test2, self()),
    {ok, Broker} = cbroker_pool:get_broker(magic2, handler),
    Requests = #{},
    run_test2_loop(Broker, Requests, 0).

run_test2_loop(Broker, Requests, RequestsPerLoop) ->
    NrOfRequest = prob_round(RequestsPerLoop),
    Requests2 = do_asks(Broker, Requests, NrOfRequest),

    case flush_inbox(Requests2, RequestsPerLoop) of
        {continue, Requests3, UpdatedRequestsPerLoop} ->
            receive
            after 1 -> ok
            end,
            run_test2_loop(Broker, Requests3, UpdatedRequestsPerLoop);
        %
        stop ->
            exit(normal)
    end.

%    ReceiveTimeout = millis_until(NextAskTimeout),
%
%    receive
%        {Tag, _} when is_map_key(Tag, Requests) ->
%            UpdatedRequests = maps:remove(Tag, Requests),
%            run_test2_loop(Broker, UpdatedRequests, NextAskTimeout, MillisBetweenAsks);
%        %
%        {set_rate, Rate} ->
%            logger:notice("Updating rate to ~p rps", [Rate]),
%
%            case Rate of
%                infinity ->
%                    run_test2_loop(Broker, Requests, infinity, infinity);
%                %
%                _ ->
%                    %NewMillisBetweenAsks = erlang:convert_time_unit(1, millisecond, native) / Rate,
%                    NewMillisBetweenAsks = 1000 / Rate,
%                    NewAskTimeout = millis_now() + prob_round(NewMillisBetweenAsks),
%                    run_test2_loop(Broker, Requests, NewAskTimeout, NewMillisBetweenAsks)
%            end;
%        %
%        stop ->
%            exit(normal)
%    after
%        1 ->
%            UpdatedRequests = async_ask(Broker, Requests),
%            NewAskTimeout = millis_now() + prob_round(MillisBetweenAsks),
%            run_test2_loop(Broker, UpdatedRequests, NewAskTimeout, MillisBetweenAsks)
%    end.

do_asks(Broker, Requests, Amount) when Amount > 0 ->
    UpdatedRequests = async_ask(Broker, Requests),
    do_asks(Broker, UpdatedRequests, Amount - 1);
do_asks(_, Requests, 0) ->
    Requests.

flush_inbox(Requests, RequestsPerLoop) ->
    receive
        {Tag, _} when is_map_key(Tag, Requests) ->
            UpdatedRequests = maps:remove(Tag, Requests),
            flush_inbox(UpdatedRequests, RequestsPerLoop);
        %
        {set_rate, Rate} ->
            NewRequestsPerLoop = Rate / 1000,
            flush_inbox(Requests, NewRequestsPerLoop);
        %
        stop ->
            stop
    after 0 ->
        {continue, Requests, RequestsPerLoop}
    end.

async_ask(Broker, Requests) ->
    case cbroker_nif:ask(Broker, left, {self(), {sleep_between, 50, 100}}, async) of
        {await, Tag} ->
            Requests#{Tag => v};
        %
        retry ->
            async_ask(Broker, Requests)
    end.

prob_round(MillisBetweenAsks) ->
    ProbOfRoundingUp = math:fmod(MillisBetweenAsks, 1),

    case (1.0 - rand:uniform()) =< ProbOfRoundingUp of
        true ->
            ceil(MillisBetweenAsks);
        %
        false ->
            floor(MillisBetweenAsks)
    end.
