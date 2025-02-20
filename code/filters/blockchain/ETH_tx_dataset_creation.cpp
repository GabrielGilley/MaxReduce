#include <iostream>
#include "ETH_BASED_tx_dataset_creation.hpp"

#include "filter.hpp"
#include "dbkey.h"
#include <boost/algorithm/string.hpp>


#include <string>
#include <sstream>
#include <iomanip>

using namespace std;
using namespace pando;

#define CRYPTO_TAG "ETH"
#define FILTER "_tx_dataset_creation"
#define FILTER_NAME CRYPTO_TAG FILTER
static const char* crypto_tag = CRYPTO_TAG;
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";


void run_(const DBAccess *access) {
    return eth_based_tx_dataset_creation(access, crypto_tag, filter_name, filter_fail_tag, filter_done_tag);
}

// Fit the Pando API
extern "C" {
    /** @brief Main filter entry point */
    extern void run(void* access) {
        // Run internally
        run_((DBAccess*)access);
    }

    extern bool should_run(const DBAccess* access) {
        return should_run_eth_based_tx_dataset_creation(access, crypto_tag, filter_fail_tag, filter_done_tag);
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

