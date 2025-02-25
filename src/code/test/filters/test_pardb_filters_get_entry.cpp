#include <iostream>
#include <string>

#include "filter.hpp"
#include "dbkey.h"

using namespace std;
using namespace pando;

static const char* filter_name = "test_pardb_filters_get_entry";
static const char* filter_done_tag = "test_pardb_filters_get_entry:done";
static const char* filter_fail_tag = "test_pardb_filters_get_entry:fail";

void run_(const DBAccess *access) {
    char* ret = nullptr;
    dbkey_t search_key {2,4,4};
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &ret);
    if (ret == nullptr) {
        access->add_tag.run(&access->add_tag, "NOT_FOUND");
        access->add_tag.run(&access->add_tag, filter_fail_tag);
        return;
    }

    string ret_s;
    ret_s.assign(ret);
    free(ret);

    string expected = "test2";
    if (ret_s != "test2") {
        cerr << "Expected:" << expected << " instead got:" << ret_s << endl; 
        throw runtime_error("Expected value not found");
    }
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
        bool will_run = false;
        while ((*tags)[0] != '\0') {
            if (string(*tags) == filter_fail_tag)
                return false;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == "A")
                will_run = true;
            ++tags;
        }
        return will_run;
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

