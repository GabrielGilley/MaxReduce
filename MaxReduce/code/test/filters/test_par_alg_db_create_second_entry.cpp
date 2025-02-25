#include "filter.hpp"

#include "terr.hpp"

#include <chrono>
#include <thread>

#include <iomanip>

#define FILTER_NAME "test_par_alg_db_create_second_entry"
static const char* filter_name = FILTER_NAME;

void run_(const DBAccess *access) {

    dbkey_t new_key = {1,5,999};
    const char* new_tags[] = {""};
    const char* new_value = "";
    access->make_new_entry.run(&access->make_new_entry, new_tags, new_value, new_key);

    access->add_tag.run(&access->add_tag, "create_second_entry:done");

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
        bool has_a = false;
        while (*tags[0] != '\0') {
            if (string(*tags) == "create_second_entry:done") return false;
            if (string(*tags) == "A") has_a = true;
            ++tags;
        }

        return has_a;
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

