#include <iostream>

#include "filter.hpp"

#include <string>

using namespace std;

// Fit the Pando API
extern "C" {
    /** @brief Main filter entry point */
    extern void run([[maybe_unused]] void* access) { }

    extern bool should_run([[maybe_unused]] const DBAccess* access) {
        return false;
    }

    /** @brief Contains the entry point and tags for the filter */
    extern const FilterInterface filter {
        filter_name: "simple",
        filter_type: SINGLE_ENTRY,
        should_run: &should_run,
        init: nullptr,
        destroy: nullptr,
        run: &run
    };
}
