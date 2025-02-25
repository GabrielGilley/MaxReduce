#include "filter.hpp"

#include "terr.hpp"

#include <chrono>
#include <thread>

#include <iomanip>

#define FILTER_NAME "test_par_alg_db_add_first_entry_tag"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";

void run_(const DBAccess *access) {

    access->add_tag.run(&access->add_tag, "A");
    access->add_tag.run(&access->add_tag, filter_done_tag);

}

// Fit the Pando API
extern "C" {

    /** @brief Main filter entry point */
    extern void run(void* access) {
        // Run internally
        run_((DBAccess*)access);
    }

    extern bool should_run(const DBAccess* access) {
        auto tags = access->tags;
        while (*tags[0] != '\0') {
            if (string(*tags) == filter_done_tag) return false;
            ++tags;
        }

        dbkey_t key = access->key;

        // only run on one specific entry
        return (key.a == 1 && key.b == 1 && key.c == 5);
    }

    /** @brief Contains the entry point and tags for the filter */
    extern const FilterInterface filter {
        filter_name: filter_name,
        filter_type: SINGLE_ENTRY,
        should_run: &should_run,
        init: nullptr,
        destroy: nullptr,
        run: &run
    };
}

