#include <iostream>
#include <string>

#include "filter.hpp"
#include "dbkey.h"

using namespace std;
using namespace pando;

static const char* filter_name = "test_pardb_filters_subscribe_and_create";
static const char* filter_done_tag = "test_pardb_filters_subscribe_and_create:done";

void run_(const DBAccess *access) {
    char* ret = nullptr;
    dbkey_t search_key {2,2,2};
    string subscribe_tag = "wait";

    access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &ret);
    string ret_s;
    if (ret != nullptr) {
        ret_s.assign(ret);
        free(ret);
    } else {
        access->add_tag.run(&access->add_tag, subscribe_tag.c_str());
        cout << "subscribing with tag=" << subscribe_tag << "  and key=" << search_key.a << ":" << search_key.b << ":" << search_key.c << endl;
        access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key, subscribe_tag.c_str());
        return;
    }

    string expected = "result";
    if (ret_s != expected) {
        cerr << "Expected:" << expected << " instead got:" << ret_s << endl; 
        throw runtime_error("Expected value not found");
    }

    const char* new_tags[] = {"B", "tag2", ""};
    const char* new_value = "result";
    dbkey_t new_key {3,3,3};
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
            if (string(*tags) == "wait")
                return false;
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

