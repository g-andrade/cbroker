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

-define(CASE_TIMEOUT, 5000).
%-define(CASE_TIMEOUT, 155).

%% ------------------------------------------------------------------
%% Type Definitions
%% ------------------------------------------------------------------

-record(bcase, {
    id :: binary(),
    implementation,
    proc_count :: pos_integer(),
    offer_type :: term(),
    offer :: term()
}).

-record(run, {
    nr_of_schedulers,
    cases,
    stats
}).

-record(result, {
    implementation,
    offer_type,
    proc_count,
    stats
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

cases(Implementations) ->
    AllOffers = [{OfferType, new_offer(OfferType)} || OfferType <- ?ALL_OFFER_TYPES],

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
            new_case(Implementation, OfferType, Offer, ProcCount)
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
        stats = StatsAcc
    },

    run_epoch(ShuffledCases, Run).

organized_stats(#run{stats = Stats}) ->
    Grouped =
        maps:to_list(
            maps:groups_from_list(
                fun({_CaseId, [CaseResult]}) ->
                    #result{
                        implementation = Implementation,
                        offer_type = OfferType
                    } = CaseResult,

                    {Implementation, OfferType}
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
            {Implementation, OfferType} = GroupKey,
            dump_group(Path, Implementation, OfferType, Entries)
        end,
        GroupedAndSorted
    ).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

new_case(Implementation, OfferType, Offer, ProcCount) ->
    #bcase{
        id = new_case_id(Implementation, OfferType, ProcCount),
        implementation = Implementation,
        proc_count = ProcCount,
        offer_type = OfferType,
        offer = Offer
    }.

new_case_id(Implementation, OfferType, ProcCount) ->
    OfferTypeIo = offer_type_io(OfferType),
    IoData = io_lib:format("~ts - ~ts (~b procs)", [Implementation, OfferTypeIo, ProcCount]),
    <<_/bytes>> = unicode:characters_to_binary(IoData).

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
        stats = StatsAcc
    } = Acc,

    #bcase{
        id = Id,
        implementation = Implementation,
        proc_count = ProcCount,
        offer = Offer
    } = Case,

    Progress = progress_str(Next, Cases),

    logger:notice("Running '~ts' [~ts]", [Id, Progress]),
    RunStats = cbroker_quickbench:bench1(Implementation, Offer, ?CASE_TIMEOUT, ProcCount),

    Result = #result{
        implementation = Implementation,
        offer_type = Case#bcase.offer_type,
        proc_count = ProcCount,
        stats = RunStats
    },

    UpdatedStatsAcc = StatsAcc#{Id := [Result]},
    UpdatedAcc = Acc#run{stats = UpdatedStatsAcc},

    run_epoch(Next, UpdatedAcc);
run_epoch([], Acc) ->
    Acc.

progress_str(Next, Cases) ->
    LenNext = length(Next),
    LenCases = length(Cases),
    Prog = LenCases - LenNext,
    Percent = 100 * Prog div LenCases,

    case time_left_str(LenNext) of
        none ->
            io_lib:format("~b / ~b (~b %)", [Prog, LenCases, Percent]);
        %
        EstimateStr ->
            io_lib:format("~b / ~b (~b %, ~ts remaining)", [Prog, LenCases, Percent, EstimateStr])
    end.

time_left_str(LenNext) ->
    ExpectationInSeconds = ceil(LenNext * (?CASE_TIMEOUT + 150) / 1000),

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

dump_group(BasePath, Implementation, OfferType, Entries) ->
    Path = filename:join([
        BasePath,
        io_lib:format("~p_~ts.csv", [Implementation, offer_type_io(OfferType)])
    ]),

    ok = filelib:ensure_dir(Path),

    logger:notice("Dumping \"~ts\"", [Path]),

    Headers =
        [
            proc_count,
            {rps, average},
            {rps, median},
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
csv_header({rps, StatName}) ->
    io_lib:format("rps_~p", [StatName]);
csv_header({delay_us, DelayType, Metric}) ->
    io_lib:format("~p_delay_~p_μs", [DelayType, Metric]).

csv_cell(ProcCount, Stats, HeaderName) ->
    case HeaderName of
        proc_count ->
            integer_to_list(ProcCount);
        %
        {rps, StatName} ->
            {_, RpsStats} = lists:keyfind(requests_per_second, 1, Stats),

            case RpsStats of
                not_available ->
                    "";
                %
                RpsStats when StatName =:= average ->
                    {_, Value} = lists:keyfind(average, 1, RpsStats),
                    integer_to_binary(Value);
                %
                RpsStats ->
                    {_, Percentiles} = lists:keyfind(percentiles, 1, RpsStats),
                    {_, Value} = lists:keyfind(StatName, 1, Percentiles),
                    integer_to_binary(Value)
            end;
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
