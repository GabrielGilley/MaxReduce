#include <iostream>
#include <string>

#include "filter.hpp"
#include "dbkey.h"

using namespace std;
using namespace pando;

static const char* filter_name = "test_pardb_filters_create";
static const char* filter_done_tag = "test_pardb_filters_create:done";

void run_(const DBAccess *access) {

    const char* new_tags[] = {"B", "tag2", ""};
    const char* new_value = "result";
    dbkey_t new_key {2,2,2};
    access->make_new_entry.run(&access->make_new_entry, new_tags, new_value, new_key);

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
        bool tag_a_found = false;
        while ((*tags)[0] != '\0') {
            if (string(*tags) == "A")
                tag_a_found = true;
            if (string(*tags) == filter_done_tag)
                return false;
            ++tags;
        }
        return tag_a_found;
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

