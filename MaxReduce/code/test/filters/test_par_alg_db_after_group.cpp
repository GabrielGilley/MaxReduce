#include "filter.hpp"

#include "terr.hpp"

#include <chrono>
#include <thread>

#include <iomanip>

#define FILTER_NAME "test_par_alg_db_after_group"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";

void run_(const DBAccess *access) {
    // create two new entries that can be used in the "final_group" test filter
    const char* new_tags1[] = {"A", ""};
    const char* new_tags2[] = {"A", ""};

    const char* new_value1 = "";
    const char* new_value2 = "";

    dbkey_t k1 = {2,2,6};
    dbkey_t k2 = {2,6,888};

    access->make_new_entry.run(&access->make_new_entry, new_tags1, new_value1, k1);
    access->make_new_entry.run(&access->make_new_entry, new_tags2, new_value2, k2);

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
        bool has_foo = false;
        while (*tags[0] != '\0') {
            if (string(*tags) == filter_done_tag) return false;
            if (string(*tags) == "FOO") has_foo = true;
            ++tags;
        }

        return has_foo;
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

