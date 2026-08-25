#ifndef OMAP_TEST_ALLOC_H
#define OMAP_TEST_ALLOC_H

#include <stddef.h>

/* Force-included into cbroker_omap.c by the test build (see Makefile), which
 * points CBROKER_OMAP_ALLOC/FREE at these so the harness can count
 * allocations and inject failures. */
void* omap_test_alloc(size_t size);
void omap_test_free(void* ptr);

#endif /* OMAP_TEST_ALLOC_H */
