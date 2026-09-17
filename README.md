# cbroker

[![](https://img.shields.io/hexpm/v/cbroker.svg?style=flat)](https://hex.pm/packages/cbroker)
[![](https://github.com/g-andrade/cbroker/actions/workflows/ci.yml/badge.svg)](https://github.com/g-andrade/cbroker/actions/workflows/ci.yml)
[![Erlang Versions](https://img.shields.io/badge/Supported%20Erlang%2FOTP-24%20to%2029-blue)](https://www.erlang.org)

cbroker provides brokers: known execution points through which BEAM processes
can message each other. These are useful for worker pools and suchlike
applications.

Rather than provide a single process that does that (commonly a `gen_server`),
`cbroker` runs concurrently through NIF code that uses [C atomics](https://en.cppreference.com/c/header/stdatomic).

It's inspired by [`sbroker`](https://hex.pm/packages/sbroker).

## Usage

TODO

## Architecture

### Overview (top-to-bottom)

#### Asks

This represents a process that wishes to either enqueue its offer on one side
of the **broker**, or instead get a counter-offer from the opposite side (a
match).

#### Broker

A broker is a ref-counted NIF resource used by asks.

It consists of:
* a **global state**;
* one or more **local states**.

It will also optionally monitor the process that created it, and close the
broker if that process dies; this will close the broker to new asks,
and cancel all enqueued requests one at a time.

#### Global state

The global state contains:
* a mutex
* a sorted collection of all checked-out **batches**
* a pool of free batches

The mutex guards all accesses to the global state.

The collection of batches keeps at least one entry at all times.

#### Local states

The local state is picked based on the running thread.

It's lock-free, and there is one per regular VM scheduler.

A local state contains:
* a sorted collection of **batches** currently **checked-out** of the global state;
* the batch ID of the left tail;
* the batch ID of the right tail;
* pools of free **requests**, environments (`ErlNifEnv`), and **tags**.

Like in global state, the collection of batches keeps at least one entry at all
times.

#### Batches and cells

A batch consists of:
* a `ref_count` to track **check-outs**;
* a few other atomic counters;
* an array of cells.

Each cell is an atomically compare-exchanged (CAS) pointer to a **request**.

#### Requests and tags

A request can be a sentinel value that signals a consumed cell.

Otherwise, it must be either nothing (empty), or an enqueued request.

An enqueued request also points to a tag, and the tag monitors the calling process.

### Details (bottom-to-top)

#### Tags

These are ref-counted ([resource
objects](https://www.erlang.org/doc/apps/erts/erl_nif.html#functionality) used
to monitor the calling process. They also provide a reference to cancel an
enqueued request.

A tag consists of:
* a monitor ([`ErlNifMonitor`](https://www.erlang.org/doc/apps/erts/erl_nif.html#data-types);
* a pointer to a **request**.

#### Requests

These are C structs used to exchange offers from the `left` side with offers
from the `right` side.

A request consists of:
* the calling pid;
* a copy of its `offer`;
* a reference to the broker;
* the location of the cell;
* a pointer to **tag**.

#### Cells

A cell is an [atomically
compare-and-swapped](https://en.cppreference.com/c/atomic/atomic_compare_exchange)
pointer to a request.

It's through this single point that a request can either enqueue, or instead
take ownership of a request from the opposite side.

At any given time, a cell is in one of five states:
1) empty;
2) enqueued request;
3) matched;
4) cancelled;
5) closed.

The 'empty' state is `NULL`.

The 'matched', 'cancelled', and 'closed' states are pointers to static sentinel
values.

Only the enqueued request is dynamically allocated.

##### Swap algorithm

In pseudo-Python, with `load` and `compare_and_swap` being atomic operations:

```Python
## Are we the first to change the cell?

counter_request = cell.load()

if counter_request is None:
    if request is None:
        tag = new Tag(self)
        request = new Request(self, tag, offer) # will copy our offer
        tag.request = request
    else:
        request.location = cell_location

    counter_request = cell.compare_and_swap(counter_request, request):

    if counter_request is None:
        # Enqueued
        return Await(tag)

## We're definitely second.

if counter_request in [SENTINEL_CANCELLED, SENTINELL_CLOSED]:
    return Skip(request, tag)

cell_request = cell.compare_and_swap(counter_request, SENTINEL_MATCHED)

if cell_request is counter_request:
    # Matched
    counter_tag = counter_request.tag

    if counter_tag.demonitor():
        tag.demonitor()

        if request is None:
            send(counter_request.pid, offer)
        else:
            # Re-use copy of our offer that we ended up not placing in cell
            send_using_request_env(counter_request.pid, request)

        release(tag)
        release(request)

        counter_offer = copy(counter_request.offer)
        release(counter_tag)
        release(counter_request)

        return Matched(counter_offer)

    else:
        # Too late, the other process died
        release(counter_tag)
        return Skip(request, tag)
```

### Batches

A batch contains an array of cells. In addition to the `ref_count`, it also contains:
* `left_count`
* `right_count`
* `consumed_count`

#### Tracking batch tails

`left_count` and `right_count` are cell position counters. Each points to the
tail of its respective side.

A position counter is atomically incremented for every ask. If its value is
`>=` that of the amount of cells in the batch, this signals that the batch is
full on that side.

#### Consuming a batch

`consumed_count` indicates how many cells were used in that batch. It's
atomically incremented when either of 2 things happen:
1. a successful match;
2. a successful cancellation.

It's also atomically set to its maximum when the broker closes.

If its value is `>=` that of the amount of cells in the batch, this signals
that the batch was consumed and is to be discarded.

#### Skipping cells

Whenever an ask encounters a cancelled cell, it will re-increment the
respective counter and try again.

It will do this up to a number of times before giving the NIF an opportunity to
reschedule.

If it sees a full batch, the ask will advance to the **next batch in the
sequence**.

If it sees a consumed batch, it will advance as well as **discard the batch**
from the local sequence.

### Batch sequences

#### Local state

Each local state contains:
* a sorted collection of batches
* `left_id`
* `right_id`

An ask starts with a batch in its local state, using either `left_id` or `right_id`.

Both point to the batch containing the tail of its respective side.

When the ask encounters a full or consumed batch, it will look for the next
batch in the sorted collection.

If it finds one, it will update the respective ID; if not, it will **checkout
the next batch** from global state.

Any remaining batches with ID lower than `min(left_id, right_id)` can be
**discarded** from the local state.


#### Global state

The global state contains:
* a mutex
* a sorted collection of _all_ batches with `ref_count` >= 2.
* a pool of free batches

All accesses to it are guarded by the mutex.

##### Checking-out of the next batch

Using the previous batch ID from the local state, we look up for a batch
with a larger ID.

If we find one, we'll increment its `ref_count` and place a copy in the local state.

If we don't find one, we'll assign (previous ID + 1) to a free batch, set its
`ref_count` to 2, and place in both global and local states.

To avoid allocating a batch in the critical section, the free batch will
usually come from the pool.

##### Discarding a batch

When a batch is discarded from a local state, its `ref_count` is lowered.

When `ref_count` reaches 1, we lock into the global state and re-check.

If the batch is already gone, that the batch ID was already removed everywhere.

If the ID is present and the `ref_count` is still 1, we remove it from the collection.

If, after removal, there's no larger ID left in the collection, we reset the
discarded batch, assign it (ID + 1), and place it batck in the collection. This
both prevents batch IDs from being reused, as well as an additional allocation.

Otherwise, we return the discarded batch to the pool, which free the batch if
full, or retain it.

### On cancellations

Cancelling an enqueued request is done through a `tag`.

We start by demonitoring: if this fails, it's too late.

Otherwise, we now have implicit ownership of the tag. Now we try to CAS the
original `cell` with the sentinel request signalling cancellation.

If this succeeds, now we have ownership of the `request` and can return it to
the pool, as well as release our own tag.

If it fails, it is also too late: another thread tried taking ownership of the
`match`, failed to demonitor, and therefore released the tag.

### On triggered monitors

It's similar to cancelling, with the caveat that the triggering of the monitor
now gives the callback implicit ownership of the `tag`.

Therefore, one of two things must now happen:

#### A) we successfully CAS the original cell

Therefore also taking ownership of the `match`, allowing us to release our own
tag, and return the match to the pool.

#### B) we're too late

Whoever tried to take ownership of the `match` failed to then demonitor, and
therefore released us (or is about to).

## License

MIT License

Copyright (c) 2026 Guilherme Andrade

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
