-record(shared_state, {
    broker :: cbroker_nif:broker(),
    broker2 :: cbroker_nif2:broker(),
    broker3 :: cbroker_nif3:broker(),
    instance :: integer()
}).
