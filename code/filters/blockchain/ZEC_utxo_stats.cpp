#include "BTC_BASED_utxo_stats.hpp"

/* ========================================================

   This is the tag of the actual blockchain, not the blockchain it was forked from.
   Edit this tag to the new crypto tag if creating filters for a new BTC fork.
   This is the only edit needed.

   ======================================================== */
#define CRYPTO_TAG "ZEC"

#define FILTER "_utxo_stats"
#define FILTER_NAME CRYPTO_TAG FILTER

static const char* crypto_tag = CRYPTO_TAG;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";

void run_(const DBAccess *access) {
    return btc_based_utxo_stats(access, crypto_tag, filter_done_tag);
}

// Fit the Pando API
extern "C" {
    /** Pass through the access as state */
    extern void* init(DBAccess* access) { return (void*)access; }
    extern void destroy([[maybe_unused]] void* state) { }

    /** @brief Main filter entry point */
    extern void run(void* access) {
        // Run internally
        run_((DBAccess*)access);
    }

    extern bool should_run(const DBAccess* access) {
        return should_run_btc_based_utxo_stats(access, crypto_tag, filter_done_tag, filter_fail_tag);
    }

    /** @brief Contains the entry point and tags for the filter */
    extern const FilterInterface filter {
        filter_name: FILTER_NAME,
        filter_type: GROUP_ENTRIES,
        should_run: &should_run,
        init: &init,
        destroy: &destroy,
        run: &run
    };
}

