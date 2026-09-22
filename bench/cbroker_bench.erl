-module(cbroker_bench).

-ifdef(E48).
-moduledoc "FIXME: one-line summary of this module.".
-endif.

-include_lib("stdlib/include/assert.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    cases/1,
    run/1,
    organized_stats/1,
    dump_stats/2
]).

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------

-define(ALL_TUPLE_OFFER_TYPES, [{tuple, N} || N <- [5, 10, 50, 100, 1000, 10000]]).
-define(ALL_LIST_OFFER_TYPES, [{list, N} || N <- [5, 10, 50, 100, 1000, 10000]]).

-define(ALL_OFFER_TYPES, [pid | ?ALL_TUPLE_OFFER_TYPES] ++ ?ALL_LIST_OFFER_TYPES).
%-define(ALL_OFFER_TYPES, [pid, {tuple, 10}]).

-define(MAX_TOTAL_ITERATIONS, 1_000_000).
%-define(TOTAL_ITERATIONS, 100).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

-record(bcase, {
    id :: binary(),
    implementation,
    proc_count :: pos_integer(),
    offer_type :: term(),
    offer :: term(),
    total_iterations :: pos_integer()
}).

-record(run, {
    nr_of_schedulers,
    cases,
    stats,
    prev_run_times :: [number()]
}).

-record(result, {
    implementation,
    offer_type,
    total_iterations,
    proc_count,
    stats
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

cases(Implementations) ->
    AllOffers = [{OfferType, new_offer(OfferType)} || OfferType <- ?ALL_OFFER_TYPES],
    IterationsPerOffer = estimate_iterations_per_offer(AllOffers),

    NrOfSchedulers = erlang:system_info(schedulers),
    AllProcCounts = lists:usort(
        [
            NrOfSchedulers,
            NrOfSchedulers * 2,
            NrOfSchedulers * 4
        ] ++
            lists:seq(100, 1000, 100) ++
            lists:seq(1000, 10_000, 1000) ++
            lists:seq(2, 2 * NrOfSchedulers)
    ),

    lists:filter(
        fun(Case) ->
            Case =/= impossible
        end,
        [
            new_case(Implementation, OfferType, Offer, ProcCount, IterationsPerOffer)
         || Implementation <- Implementations,
            {OfferType, Offer} <- AllOffers,
            ProcCount <- AllProcCounts
        ]
    ).

run(Cases) ->
    CaseIds = [Case#bcase.id || Case <- Cases],
    StatsAcc = maps:from_keys(CaseIds, []),
    ShuffledCases = rand:shuffle(Cases),

    Run = #run{
        nr_of_schedulers = erlang:system_info(schedulers),
        cases = Cases,
        stats = StatsAcc,
        prev_run_times = []
    },

    run_epoch(ShuffledCases, Run).

organized_stats(#run{stats = Stats}) ->
    Grouped =
        maps:to_list(
            maps:groups_from_list(
                fun({_CaseId, [CaseResult]}) ->
                    #result{
                        implementation = Implementation,
                        offer_type = OfferType,
                        total_iterations = TotalIterations
                    } = CaseResult,

                    {Implementation, OfferType, TotalIterations}
                end,
                maps:to_list(Stats)
            )
        ),

    _GroupedAndSorted = lists:keysort(1, [
        {GroupKey, prepare_group(Group)}
     || {GroupKey, Group} <- Grouped
    ]).

dump_stats(Path, GroupedAndSorted) ->
    lists:foreach(
        fun({GroupKey, Entries}) ->
            {Implementation, OfferType, TotalIterations} = GroupKey,
            dump_group(Path, Implementation, OfferType, TotalIterations, Entries)
        end,
        GroupedAndSorted
    ).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

estimate_iterations_per_offer(AllOffers) ->
    {ok, _} = application:ensure_all_started([taskforce]),
    IndividualTimeout = 5000,

    TaskList = lists:map(
        fun({OfferType, Offer}) ->
            Task = taskforce:task(fun estimate_offer_iterations/1, [Offer], #{
                timeout => IndividualTimeout
            }),
            {OfferType, Task}
        end,
        AllOffers
    ),

    Tasks = maps:from_list(TaskList),

    logger:notice("Estimating iterations per offer..."),
    #{
        completed := Results,
        individual_timeouts := [],
        global_timeouts := []
    } = taskforce:execute(Tasks, #{timeouts => IndividualTimeout * length(AllOffers)}),

    Rates = maps:values(Results),
    {copy_rate, MaxRate} = lists:max(Rates),

    maps:map(
        fun(_OfferType, {copy_rate, CopyRate}) ->
            RelativeRate = CopyRate / MaxRate,
            ?assertMatch(_ when RelativeRate > 0, RelativeRate =< 1, RelativeRate),
            RawTotalIterations = ceil(RelativeRate * ?MAX_TOTAL_ITERATIONS),
            TotalIterations = nicer_integer(RawTotalIterations),
            {total_iterations, TotalIterations}
        end,
        Results
    ).

estimate_offer_iterations(Offer) ->
    StartTs = erlang:monotonic_time(),
    Parent = self(),
    AuxPid = spawn_link(fun() -> offer_estimator_aux(Parent) end),
    _ = erlang:send_after(2000, self(), finish),
    estimate_offer_iterations(AuxPid, Offer, StartTs, 0).

estimate_offer_iterations(AuxPid, Offer, StartTs, Copies) ->
    _ = AuxPid ! {first_copy, Offer},

    receive
        Msg ->
            case Msg of
                {second_copy, _} ->
                    estimate_offer_iterations(AuxPid, Offer, StartTs, Copies + 2);
                %
                finish ->
                    FinishTs = erlang:monotonic_time(),
                    ElapsedSeconds =
                        (FinishTs - StartTs) / erlang:convert_time_unit(1, second, native),
                    CopyRate = (Copies + 1) / ElapsedSeconds,
                    {copy_rate, CopyRate}
            end
    end.

offer_estimator_aux(Parent) ->
    receive
        Msg ->
            case Msg of
                {first_copy, Offer} ->
                    Parent ! {second_copy, Offer},
                    offer_estimator_aux(Parent)
            end
    end.

nicer_integer(Iterations) ->
    Exponent = floor(math:log10(Iterations)) - 2,

    case Exponent < 0 of
        true ->
            Iterations;
        %
        false ->
            Divisor = trunc(math:pow(10, Exponent)),
            (Iterations div Divisor) * Divisor
    end.

%%

new_case(Implementation, OfferType, Offer, ProcCount, IterationsPerOffer) ->
    TotalIterations = total_iterations(OfferType, ProcCount, IterationsPerOffer),

    case TotalIterations < ProcCount of
        true ->
            impossible;
        %
        false ->
            #bcase{
                id = new_case_id(Implementation, OfferType, ProcCount),
                implementation = Implementation,
                proc_count = ProcCount,
                offer_type = OfferType,
                offer = Offer,
                total_iterations = TotalIterations
            }
    end.

new_case_id(Implementation, OfferType, ProcCount) ->
    OfferTypeIo = offer_type_io(OfferType),
    IoData = io_lib:format("~ts - ~ts (~b procs)", [Implementation, OfferTypeIo, ProcCount]),
    <<_/bytes>> = unicode:characters_to_binary(IoData).

total_iterations(OfferType, ProcCount, IterationsPerOffer) ->
    {total_iterations, TotalIterations} = maps:get(OfferType, IterationsPerOffer),

    case TotalIterations >= ProcCount of
        true ->
            TotalIterations;
        %
        false ->
            impossible
    end.

offer_type_io(pid) ->
    "pid";
offer_type_io({tuple, Size}) ->
    io_lib:format("tuple~b", [Size]);
offer_type_io({list, Size}) ->
    io_lib:format("list~b", [Size]).

new_offer(pid) ->
    self();
new_offer({tuple, Size}) ->
    ?assertEqual(0, erts_debug:size(Size)),
    List = lists:seq(1, Size),
    list_to_tuple(List);
new_offer({list, Size}) ->
    lists:seq(1, Size).

run_epoch([Case | Next], Acc) ->
    #run{
        cases = Cases,
        stats = StatsAcc,
        prev_run_times = PrevRunTimes
    } = Acc,

    #bcase{
        id = Id,
        implementation = Implementation,
        proc_count = ProcCount,
        offer = Offer,
        total_iterations = TotalIterations
    } = Case,

    Progress = progress_str(Next, Cases, PrevRunTimes),

    logger:notice("Running '~ts' x~b [~ts]", [Id, TotalIterations, Progress]),
    {Time, RunStats} = timer:tc(
        fun() -> cbroker_quickbench:bench1(Implementation, Offer, TotalIterations, ProcCount) end,
        millisecond
    ),

    Result = #result{
        implementation = Implementation,
        offer_type = Case#bcase.offer_type,
        total_iterations = TotalIterations,
        proc_count = ProcCount,
        stats = RunStats
    },

    UpdatedStatsAcc = StatsAcc#{Id := [Result]},
    UpdatedAcc = Acc#run{stats = UpdatedStatsAcc, prev_run_times = [Time | PrevRunTimes]},

    run_epoch(Next, UpdatedAcc);
run_epoch([], Acc) ->
    Acc.

progress_str(Next, Cases, PrevRunTimes) ->
    LenNext = length(Next),
    LenCases = length(Cases),
    Prog = LenCases - LenNext,
    Percent = 100 * Prog div LenCases,

    case estimate_time_left_str(PrevRunTimes, LenNext) of
        none ->
            io_lib:format("~b / ~b (~b %)", [Prog, LenCases, Percent]);
        %
        EstimateStr ->
            io_lib:format("~b / ~b (~b %, ~ts remaining)", [Prog, LenCases, Percent, EstimateStr])
    end.

estimate_time_left_str(PrevRunTimes, _LenNext) when length(PrevRunTimes) < 5 ->
    none;
estimate_time_left_str(PrevRunTimes, LenNext) ->
    AvgRunTime = lists:sum(PrevRunTimes) / length(PrevRunTimes),
    ExpectationInSeconds = ceil(AvgRunTime * LenNext / 1000),

    Hours = ExpectationInSeconds div 3600,
    HourSeconds = ExpectationInSeconds rem 3600,

    Minutes = HourSeconds div 60,
    Seconds = HourSeconds rem 60,

    if
        Hours =/= 0 ->
            io_lib:format("~bh~bm", [Hours, Minutes]);
        %
        Minutes =/= 0, Seconds =/= 0 ->
            io_lib:format("~bm~bs", [Minutes, Seconds]);
        %
        Minutes =/= 0 ->
            io_lib:format("~bm", [Minutes]);
        %
        true ->
            io_lib:format("~bs", [Seconds])
    end.

%%

prepare_group(Entries) ->
    PreparedEntries = [prepare_group_entry(Entry) || Entry <- Entries],
    lists:keysort(1, PreparedEntries).

prepare_group_entry({_CaseId, [CaseResult]}) ->
    #result{
        proc_count = ProcCount
    } = CaseResult,

    {ProcCount, CaseResult}.

%%

dump_group(BasePath, Implementation, OfferType, TotalIterations, Entries) ->
    Path = filename:join([
        BasePath,
        atom_to_binary(Implementation, utf8),
        io_lib:format("~ts x~b.csv", [offer_type_io(OfferType), TotalIterations])
    ]),

    ok = filelib:ensure_dir(Path),

    logger:notice("Dumping \"~ts\"", [Path]),

    Headers =
        [
            proc_count,
            run_time_s,
            {delay_us, blocked, average},
            {delay_us, blocked, median},
            {delay_us, blocked, p95},
            {delay_us, blocked, p99},
            {delay_us, instant, average},
            {delay_us, instant, median},
            {delay_us, instant, p95},
            {delay_us, instant, p99}
        ],

    Rows =
        lists:map(
            fun({_ProcCount, #result{proc_count = ProcCount, stats = Stats}}) ->
                CsvCells = [csv_cell(ProcCount, Stats, Header) || Header <- Headers],
                lists:join($,, CsvCells)
            end,
            Entries
        ),

    HeaderNames = [csv_header(Header) || Header <- Headers],

    CsvData = unicode:characters_to_binary([
        lists:join($,, HeaderNames),
        $\n,
        [[Row, $\n] || Row <- Rows]
    ]),

    ok = file:write_file(Path, CsvData).

csv_header(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
csv_header({delay_us, DelayType, Metric}) ->
    io_lib:format("~p_delay_~p_μs", [DelayType, Metric]).

csv_cell(ProcCount, Stats, HeaderName) ->
    case HeaderName of
        proc_count ->
            integer_to_list(ProcCount);
        %
        run_time_s ->
            {_, Value} = lists:keyfind(total_duration_secs, 1, Stats),
            float_to_binary(Value, [{decimals, 4}, compact]);
        %
        {delay_us, DelayType, StatName} ->
            {_, DelaysPerGroup} = lists:keyfind(delays_per_group, 1, Stats),

            case lists:keyfind(DelayType, 1, DelaysPerGroup) of
                {_, Delays} ->
                    csv_cell_delay(StatName, Delays);
                %
                false ->
                    ""
            end
    end.

csv_cell_delay(average, Delays) ->
    {_, Value} = lists:keyfind(average, 1, Delays),
    true = is_integer(Value),
    integer_to_list(Value);
csv_cell_delay(Other, Delays) ->
    {_, Percentiles} = lists:keyfind(percentiles, 1, Delays),
    {_, Value} = lists:keyfind(Other, 1, Percentiles),
    true = is_integer(Value),
    integer_to_list(Value).
