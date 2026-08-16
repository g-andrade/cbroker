#include "erl_nif.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

/*********************************************************************/

#define ATOM_LIST \
    X(_await,          "await") \
    X(_badarg,         "badarg") \
    X(_cancelled,      "cancelled") \
    X(_delayed_match,  "delayed_match") \
    X(_empty,          "empty")  \
    X(_instant_match,  "instant_match") \
    X(_left,           "left")  \
    X(_match,          "match") \
    X(_matched,        "matched") \
    X(_none,           "none") \
    X(_ok,             "ok")  \
    X(_retry,          "retry") \
    X(_right,          "right") \
    X(_todo,           "todo") \
    X(_true,           "true")


#define BATCH_NR_OF_CELLS 128

#define MAX(a, b), ((a) >= (b) ? (a) : B)

#define BATCH_ARRAY_INITIAL_CAPACITY 8

/*********************************************************************/


/*********************************************************************/

#define X(field, name) ERL_NIF_TERM field;
static struct {
    ATOM_LIST
} Atoms;
#undef X

//

static struct {
    ErlNifResourceType* broker;
} ResourceTypes;

typedef uint_fast64_t thread_id_t;
static _Atomic(thread_id_t) next_thread_id = 1;
static _Thread_local thread_id_t my_thread_id = 0;

//

typedef uint_fast64_t offset_t;
typedef _Atomic(offset_t) atomic_offset_t;

//

typedef struct {
    ErlNifEnv* env;
    ERL_NIF_TERM exchange_value;
} match_t;

static match_t* second_match;

//

typedef struct {
    _Atomic(ERL_NIF_TERM) status;
    _Atomic(void*) match;
} cell_t;

//

typedef struct {
    offset_t offset;
    atomic_size_t ref_count;
    size_t nr_of_cells;
    cell_t cells[];
} batch_t;
 
//

typedef struct {
//    ErlNifMutex* mutex;
//    batch_id_t batch_counter;
    ErlNifEnv* env;
    batch_t** batches;
    size_t capacity;
    size_t size;
} batch_array_t;

//

typedef struct {
    ErlNifMonitor owner_mon;
    size_t nr_of_schedulers;
    atomic_offset_t left_counter;
    atomic_offset_t right_counter;
    batch_array_t* batch_arrays; // 1 + nr_of_schedulers; index 0 is shared
    ErlNifMutex* mutex;
} broker_t;

/*********************************************************************/

static thread_id_t get_or_assign_thread_id(void) {
    if (my_thread_id == 0) {
        my_thread_id = atomic_fetch_add_explicit(&next_thread_id, 1, memory_order_relaxed);
    }
    assert(my_thread_id > 0);
    return my_thread_id;

    /*
     * Ask requirements:
     * - get next batch for current side
     */
}

/*********************************************************************/

static ERL_NIF_TERM nif_make_badarg2(ErlNifEnv* env, ERL_NIF_TERM term) {
    ERL_NIF_TERM reason = enif_make_tuple2(env, Atoms._badarg, term);
    return enif_raise_exception(env, reason);
}

static int nif_get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out) {
    return enif_get_resource(env, term, ResourceTypes.broker, (void**) out);
}

static ERL_NIF_TERM nif_make_ticket(ErlNifEnv* env, 
                                    ERL_NIF_TERM broker_term,
                                    ERL_NIF_TERM side,
                                    const offset_t offset) {
    return enif_make_tuple3(
        env, 
        broker_term, 
        side,
        enif_make_uint64(env, offset)
    );
}

static ERL_NIF_TERM nif_make_await(ErlNifEnv* env, ERL_NIF_TERM ticket) {
    return enif_make_tuple2(env, Atoms._await, ticket);
}

static ERL_NIF_TERM nif_make_match_msg(ErlNifEnv* env, ERL_NIF_TERM ticket, ERL_NIF_TERM exchange_value) {
    return enif_make_tuple2(env, ticket, enif_make_tuple2(env, Atoms._match, exchange_value));
}

//static int nif_get_batch(ErlNifEnv* env, ERL_NIF_TERM term, batch_t** out) {
//    return enif_get_resource(env, term, ResourceTypes.batch, (void**) out);
//}

/*********************************************************************/

static int batch_array_search(batch_array_t* array, const offset_t target_offset, size_t* out_idx, batch_t** out_batch) {
    size_t low = 0, high = array->size - 1;
    batch_t** batches = array->batches;

    while (low <= high) {
        int mid = low + (high - low) / 2;
        batch_t* mid_batch = batches[mid];
        offset_t mid_offset = mid_batch->offset;

        if (mid_offset == target_offset) {
            *out_idx = mid;
            *out_batch = mid_batch;
            return 1;
        } else if (mid_offset < target_offset) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }

    *out_idx = low;
    return 0;
}

static void batch_array_insert(batch_array_t* array, size_t idx, batch_t* batch) {
    assert(idx <= array->size);

    if (array->size == array->capacity) {
        size_t new_cap = (array->capacity == 0) ? 4 : (array->capacity * 2);
        array->batches = enif_realloc(array->batches, new_cap);
        assert(array->batches != NULL);
        array->capacity = new_cap;
    } else {
        assert(array->size < array->capacity);
    }

    size_t moved_size = (array->size - idx) * sizeof(batch_t*);
    if (moved_size != 0) {
        memmove(&array->batches[idx + 1], &array->batches[idx], moved_size);
    }

    array->batches[idx] = batch;
    array->size++;
}

static batch_t* new_batch(offset_t offset) {
    size_t struct_size = sizeof(batch_t) + (BATCH_NR_OF_CELLS * sizeof(cell_t));
    batch_t* batch = enif_alloc(struct_size);
    memset(batch, 0, struct_size);
    batch->offset = offset;
    batch->nr_of_cells = BATCH_NR_OF_CELLS;
    return batch;
}

static batch_t* broker_get_batch(broker_t* broker, const offset_t offset) {
    const thread_id_t thread_id = get_or_assign_thread_id();
    assert(thread_id <= broker->nr_of_schedulers);
    batch_array_t* local_batch_array = &broker->batch_arrays[thread_id];

    offset_t batch_offset = offset / BATCH_NR_OF_CELLS;
    batch_t* batch = NULL;
    size_t local_batch_idx = 0;
    if (batch_array_search(local_batch_array, batch_offset, &local_batch_idx, &batch)) {
        return batch;
    }

    //

    batch_array_t* shared_batch_array = &broker->batch_arrays[0];
    size_t shared_batch_idx = 0;

    enif_mutex_lock(broker->mutex);
    if (batch_array_search(shared_batch_array, batch_offset, &shared_batch_idx, &batch)) {
        atomic_fetch_add(&batch->ref_count, +1);
        enif_mutex_unlock(broker->mutex);
        batch_array_insert(local_batch_array, local_batch_idx, batch);
    } else {
        batch_t* batch = new_batch(batch_offset);
        atomic_fetch_add(&batch->ref_count, +2);
        batch_array_insert(shared_batch_array, shared_batch_idx, batch);
        enif_mutex_unlock(broker->mutex);
        batch_array_insert(local_batch_array, local_batch_idx, batch);
    }
    return batch;
}

static match_t* match_new_pending(ERL_NIF_TERM exchange_value) {
    // TODO optimize: don't allocate env for immediate terms

    match_t* match = enif_alloc(sizeof(match_t));
    memset(match, 0, sizeof(match_t));

    ErlNifEnv* new_env = enif_alloc_env();
    match->env = new_env;
    match->exchange_value = enif_make_copy(new_env, exchange_value);

    return match;
}

static void match_free(match_t* match) {
    if (match->env != NULL) {
        enif_free_env(match->env);
    }
    enif_free(match);
}

static ERL_NIF_TERM do_ask(
    ErlNifEnv* env, broker_t* broker, atomic_offset_t* head_counter, 
    ErlNifPid self, ERL_NIF_TERM exchange_value,
    //
    offset_t* out_offset, ErlNifPid* out_other_pid, match_t** out_first_match
) {
    batch_t* batch = NULL;
    ERL_NIF_TERM self_term = enif_make_pid(env, &self);

    int attempts_left = 5;

    while (attempts_left-- > 0) {
        const offset_t offset = atomic_fetch_add(head_counter, +1) - 1;

        if (batch == NULL || offset > batch->offset + batch->nr_of_cells) {
            batch = broker_get_batch(broker, offset);
        }

        const size_t cell_idx = offset - batch->offset;
        cell_t* cell = &batch->cells[cell_idx];

        ERL_NIF_TERM status = Atoms._empty;
        ErlNifPid other_pid;
        
        if (atomic_compare_exchange_strong(&cell->status, &status, self_term)) {
            void* match = NULL;
            match_t* pending_match = match_new_pending(exchange_value);

            if (atomic_compare_exchange_strong(&cell->match, &match, pending_match)) {
                // Enqueued
                *out_offset = offset;
                return Atoms._await;
            } else {
                // Instant match - the other party will message us.
                *out_offset = offset;
                assert(enif_get_local_pid(env, (ERL_NIF_TERM) match, out_other_pid));
                *out_first_match = pending_match;
                return Atoms._instant_match;
            }
        } 
        else if (
            enif_get_local_pid(env, status, &other_pid)
            && atomic_compare_exchange_strong(&cell->status, &status, Atoms._matched)
        ) {
            void* desired_match = (void*) self_term;
            void* match = atomic_exchange(&cell->match, desired_match);

            if (match == NULL) {
                // Instant match - the other party will message us.
                *out_offset = offset;
                *out_other_pid = other_pid;
                *out_first_match = NULL;
                return Atoms._instant_match;
            }
            else {
                /* Delayed Match
                     * 1) message `other_pid` with our exchange term
                     * 2) return the first exchange term
                     */
                *out_offset = offset;
                *out_first_match = (match_t*) match;
                return Atoms._delayed_match;
            }
        } 

        assert(status == Atoms._cancelled);
    }
    return Atoms._retry;
}

static void notify_other_of_match(ErlNifEnv* env,
                                  ERL_NIF_TERM broker_term, 
                                  ERL_NIF_TERM side, 
                                  ERL_NIF_TERM exchange_value,
                                  offset_t offset, 
                                  ErlNifPid* other_pid,
                                  match_t* first_match) 
{
    ERL_NIF_TERM other_side = (side == Atoms._left ? Atoms._right : Atoms._left);
    ErlNifEnv* other_env = NULL;
    ERL_NIF_TERM copied_exchange_value;

    if (first_match == NULL) {
        other_env = enif_alloc_env();
        copied_exchange_value = enif_make_copy(other_env, exchange_value);
    } else if (first_match->env == NULL) {
        other_env = enif_alloc_env();
        copied_exchange_value = first_match->exchange_value;
    } else {
        // We can reuse the env in first_match
        other_env = first_match->env;
        copied_exchange_value = first_match->exchange_value;
    }

    ERL_NIF_TERM other_broker = enif_make_copy(other_env, broker_term);
    ERL_NIF_TERM other_ticket = nif_make_ticket(other_env, other_broker, other_side, offset);
    ERL_NIF_TERM other_msg = nif_make_match_msg(other_env, other_ticket, copied_exchange_value);
    enif_send(env, other_pid, other_env, other_msg);
    enif_free_env(other_env);

    if (first_match != NULL) {
        enif_free(first_match);
    }
}

/*********************************************************************/

//static matcher_t *ensure_matcher(broker_t* broker, uint_fast64_t entry_pos) {
//    matcher_t *arr = broker->matchers;
//    size_t n = broker->matchers_count;
//    size_t lo = 0, hi = n;
//
//    // Binary search: ranges are [start, start + MATCHER_CELL_COUNT)
//    while (lo < hi) {
//        size_t mid = lo + (hi - lo) / 2;
//        uint_fast64_t mid_start = arr[mid].start;
//
//        if (entry_pos < mid_start) {
//            hi = mid;
//        } else if (entry_pos >= mid_start + MATCHER_CELL_COUNT) {
//            lo = mid + 1;
//        } else {
//            return &arr[mid];  // entry_pos falls within this matcher's range
//        }
//    }
//
//    // Not found — lo is the correct insertion index to keep the array sorted
//    if (n == broker->matchers_capacity) {
//        size_t new_cap = (broker->matchers_capacity == 0) ? 4 : (broker->matchers_capacity * 2);
//        matcher_t *resized = enif_realloc(arr, new_cap * sizeof(matcher_t));
//        if (!resized) {
//            return NULL;  // caller must handle allocation failure
//        }
//        arr = resized;
//        broker->matchers = arr;
//        broker->matchers_capacity = new_cap;
//    }
//
//    // Shift everything from lo onward to the right by one
//    if (lo < n) {
//        memmove(&arr[lo + 1], &arr[lo], (n - lo) * sizeof(matcher_t));
//    }
//
//    // Align the new matcher's start to a MATCHER_CELL_COUNT boundary
//    uint_fast64_t new_start = (entry_pos / MATCHER_CELL_COUNT) * MATCHER_CELL_COUNT;
//
//    arr[lo].start = new_start;
//
//    ErlNifEnv* matcher_env = enif_alloc_env();
//    arr[lo].env = matcher_env;
//
//    batch_t* batch = enif_alloc_resource(ResourceTypes.batch, sizeof(batch_t));
//    memset(batch, 0, sizeof(batch_t));
//    arr[lo].batch_term = enif_make_resource(matcher_env, batch);
//
//    broker->matchers_count = n + 1;
//    return &arr[lo];
//}

// TODO destroy matcher

/*********************************************************************/

//static entry_t* entry_new(ErlNifPid pid) {
//    entry_t* entry = enif_alloc(sizeof(entry_t));
//    memset(entry, 0, sizeof(entry_t));
//    entry->pid = pid;
//    return entry;
//}

//static cell_t* cell_new(ErlNifPid pid, ERL_NIF_TERM exchange_value) {
//    cell_t* cell = enif_alloc(sizeof(cell_t));
//    memset(cell, 0, sizeof(cell_t));
//
//    // TODO optimize immediate terms?
//    cell->pid = pid;
//    cell->env = enif_alloc_env();
//    cell->exchange_value = enif_make_copy(cell->env, term);
//    return cell;
//}


/*********************************************************************/

static ERL_NIF_TERM
niff_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifPid self;
    if (! enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    broker_t* broker = enif_alloc_resource(ResourceTypes.broker, sizeof(broker_t));
    memset(broker, 0, sizeof(broker_t));

    if (! enif_monitor_process(env, broker, &self, &broker->owner_mon)) {
        enif_release_resource(broker);
    }

    ErlNifSysInfo sys_info;
    enif_system_info(&sys_info, sizeof(sys_info));
    size_t nr_of_schedulers = sys_info.scheduler_threads;
    assert(nr_of_schedulers > 0);
    broker->nr_of_schedulers = nr_of_schedulers;

    size_t nr_of_batch_arrays = 1 + nr_of_schedulers;
    broker->batch_arrays = malloc(nr_of_batch_arrays * sizeof(batch_array_t));
    memset(broker->batch_arrays, 0, nr_of_batch_arrays * sizeof(batch_array_t));

    for (size_t i=0; i <= nr_of_batch_arrays; i++) {
        batch_array_t* batch_array = &broker->batch_arrays[i];
        batch_array->env = enif_alloc_env();
    }

    broker->mutex = enif_mutex_create("cbroker.mutex");

    return enif_make_resource(env, broker);
}

//

static ERL_NIF_TERM
niff_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    broker_t* broker = NULL;
    ErlNifPid self;
    atomic_offset_t* head_counter = NULL;
    int is_fully_async = 0;

    const ERL_NIF_TERM broker_term = argv[0];
    const ERL_NIF_TERM side = argv[1];
    const ERL_NIF_TERM exchange_value = argv[2];

    if (! nif_get_broker(env, broker_term, &broker)) {
        return nif_make_badarg2(env, broker_term);
    }

    if (! enif_self(env, &self)) {
        return enif_make_badarg(env);
    }
        
    if (side == Atoms._left) {
        head_counter = &broker->left_counter;
    } else if (side == Atoms._right) {
        head_counter = &broker->right_counter;
    } else {
        return nif_make_badarg2(env, side);
    }
    
    is_fully_async = (argc >= 4 && argv[3] == Atoms._true);

    ///

    offset_t offset = 0;
    ErlNifPid other_pid;
    match_t* first_match = NULL;

    ERL_NIF_TERM res = do_ask(env, broker, head_counter, self, exchange_value, &offset, &other_pid, &first_match);

    if (res == Atoms._await) 
    {
        ERL_NIF_TERM ticket = nif_make_ticket(env, broker_term, side, offset);
        return nif_make_await(env, ticket);
    } 
    else if (res == Atoms._instant_match) 
    {
        // Other party will message us
        notify_other_of_match(env, broker_term, side, exchange_value, offset, &other_pid, first_match);
        //
        ERL_NIF_TERM ticket = nif_make_ticket(env, broker_term, side, offset);
        return nif_make_await(env, ticket);
    } 
    else if (res == Atoms._delayed_match) 
    {
        assert(first_match != NULL);
        notify_other_of_match(env, broker_term, side, exchange_value, offset, &other_pid, NULL);
        //
        ERL_NIF_TERM ticket = nif_make_ticket(env, broker_term, side, offset);
        ERL_NIF_TERM copied_exchange_value = enif_make_copy(env, first_match->exchange_value);

        if (first_match->env != NULL) {
            enif_free_env(first_match->env);
        }
        enif_free(first_match);

        if (is_fully_async) {
            ERL_NIF_TERM self_msg = nif_make_match_msg(env, ticket, copied_exchange_value);
            enif_send(env, &self, NULL, self_msg);
            return nif_make_await(env, ticket);
        }
        else {
            return enif_make_tuple3(env, Atoms._match, ticket, copied_exchange_value);
        }
    } 
    else {
        assert (res == Atoms._retry);
        // TODO
    }
}


//

static ErlNifFunc nif_funcs[] = {
    {"new", 0, niff_new},
    {"ask", 3, niff_ask},
    {"ask", 4, niff_ask}
};

/*********************************************************************/

static void broker_dtor(ErlNifEnv* caller_env, void* obj) {
    broker_t* broker = (broker_t*) obj;
    //memset(obj, 0, sizeof(broker_t));
}

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon) {
    broker_t* broker = (broker_t*) obj;

    enif_mutex_lock(broker->mutex);
    batch_array_t* batch_array = &broker->batch_arrays[0];

    for (size_t i = 0; i < batch_array->size; i++) {
        batch_t* batch = batch_array->batches[i];
        for (offset_t j = 0; j < batch->nr_of_cells; j++) {
            cell_t* cell = &batch->cells[j];

            const ERL_NIF_TERM desired_status = Atoms._cancelled;
            ERL_NIF_TERM expected_status = Atoms._empty;
            ERL_NIF_TERM status = atomic_compare_exchange_strong(&cell->status, &expected_status, desired_status);

            if (enif_is_pid(caller_env, status)) {
                expected_status = status;
                status = atomic_compare_exchange_strong(&cell->status, &expected_status, desired_status);

                if (status !== desired_status) {
                    // in the mean time, matched or cancelled somewhere else
                    continue;
                }
            }
            else if (status != desired_status) {
                // too late to cancel
                continue;
            }

            // TODO send message
        }
    }
}

///////////////

/*********************************************************************/

static void init_atoms(ErlNifEnv* caller_env) {
    memset(&Atoms, 0, sizeof(Atoms));
    #define X(field, name) Atoms.field = enif_make_atom(caller_env, name);
    ATOM_LIST
    #undef X
}

static void broker_resource_load(ErlNifEnv* caller_env) {
    ErlNifResourceTypeInit callbacks = {broker_dtor, NULL, broker_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.broker = enif_init_resource_type(
        caller_env, 
        "cbroker",
        &callbacks,
        flags,
        &flags
    );
    assert(ResourceTypes.broker != NULL);
}

static int nif_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info) {
    init_atoms(caller_env);

    memset(&ResourceTypes, 0, sizeof(ResourceTypes));
    broker_resource_load(caller_env);

    return 0;
}

ERL_NIF_INIT(cbroker_nif, nif_funcs, nif_load, NULL, NULL, NULL);
