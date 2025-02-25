#include <iostream>
#include <string>

#include "filter.hpp"
#include "dbkey.h"

using namespace std;
using namespace pando;

static const char* filter_name = "test_pardb_filters_update_val";
static const char* filter_done_tag = "test_pardb_filters_update_val:done";

void run_(const DBAccess *access) {
    dbkey_t search_key {2,3,4};
    string new_val = "NEW VALUE";
    access->update_entry_val.run(&access->update_entry_val, search_key, new_val.c_str());
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
        bool a_found = false;
        while ((*tags)[0] != '\0') {
            if (string(*tags) == "A") {
                a_found = true;
            }
            if (string(*tags) == filter_done_tag)
                return false;
            ++tags;
        }
        return a_found;
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

