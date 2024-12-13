#include <iostream>
#include <sstream>
#include <string>

#include "filter.hpp"
#include "dbkey.h"

#include "gas_filter.hpp"
#include "terr.hpp"

using namespace std;
using namespace pando;

#define FILTER_NAME "DOGE_tx_subgraph_search"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";


typedef uint64_t T;
class BFS : public GASFilter<T,T> {
    private:
        const char* tags_[3];

        dbkey_t key_;
    protected:
        T G(T a, T b) { return min(a, b); }
        T A(T v, T a) { return min(v, a); }
        T S(T v) {
            if (v == numeric_limits<T>::max())
                return numeric_limits<T>::max();
            return v+1;
        }
        T gather_init([[maybe_unused]] DBAccess* acc) {
            return numeric_limits<T>::max();
        }
        T init(DBAccess* acc) {
            //TODO for now, expect an entry with key {DOGE_KEY,
            //BFS_INITIAL_VALUE, 0} as the "a key" and the first 8 bytes of
            //the starting tx id to be the b key to initialize this filter.
            //This will likely cause issues if this filter is run twice
            //with two different initializing transactions without clearing
            //the initial tx entry before rerunning.
            chain_info_t key_a = pack_chain_info(DOGE_KEY, BFS_INITIAL_VALUE, 0);
            dbkey_t search_key = {key_a, acc->key.b, 0};

            char* initial_val_ret = nullptr;
            acc->get_entry_by_key.run(&acc->get_entry_by_key, search_key, &initial_val_ret);

            T alg_val;
            string alg_val_str;
            if (initial_val_ret == nullptr) {
                alg_val = numeric_limits<T>::max();
                alg_val_str = to_string(alg_val);
            }
            else {
                alg_val_str.assign(initial_val_ret);
                free(initial_val_ret);
                alg_val = stoull(alg_val_str);
            }

            acc->make_new_entry.run(&acc->make_new_entry, tags_, alg_val_str.c_str(), key_);
            return alg_val;
        }

        void save_output(DBAccess* acc, T val) {
            string val_s = to_string(val);
            acc->update_entry_val.run(&acc->update_entry_val, key_, val_s.c_str());

            // add the done tag here to all entries in this group (TODO is this right?)
            // even though it's not quite done yet, we're expecting it to finish
            for (auto & out_edge : acc->group->entry.out) {
                out_edge.acc->add_tag.run(&out_edge.acc->add_tag, filter_done_tag);
            }
        }
        size_t it = 0;
    public:
        using GASFilter<T,T>::GASFilter;

        BFS(DBAccess* acc) : GASFilter<T,T>::GASFilter(acc) {
            chain_info_t chain_info = pack_chain_info(DOGE_KEY, DISTANCE_KEY, 0);
            auto b_key = acc->key.b;
            auto c_key = acc->key.c;
            key_ = {chain_info, b_key, c_key};

            tags_[0] = "DOGE";
            tags_[1] = "TX_DISTANCE";
            tags_[2] = "";
        }
};

// Fit the Pando API
extern "C" {
    /** @brief Construct the filter state */
    extern void* init(DBAccess* access) {
        BFS* bfs = new BFS(access);
        return (void*)bfs;
    }

    /** @brief Destroy the filter state */
    extern void destroy(void* state) {
        delete (BFS*)state;
    }

    /** @brief Main filter entry point */
    extern void run(void* state) {
        // Run internally
        BFS& bfs = *(BFS*)state;
        bfs.run();
    }

    extern bool should_run(const DBAccess* access) {
        // Only run if we have the right keys
        chain_info_t btc_ci_not_utxo = pack_chain_info(DOGE_KEY, TX_OUT_EDGE_KEY, NOT_UTXO_KEY);
        //chain_info_t btc_ci_utxo = pack_chain_info(DOGE_KEY, TX_OUT_EDGE_KEY, UTXO_KEY);
        chain_info_t zec_ci_not_utxo = pack_chain_info(ZEC_KEY, TX_OUT_EDGE_KEY, NOT_UTXO_KEY);
        //chain_info_t zec_ci_utxo = pack_chain_info(ZEC_KEY, TX_OUT_EDGE_KEY, UTXO_KEY);
        chain_info_t a = access->key.a;
        
        bool ci_good = false;
        //if (a == btc_ci_not_utxo || a == zec_ci_not_utxo || a == btc_ci_utxo || a == zec_ci_utxo)
        if (a == btc_ci_not_utxo || a == zec_ci_not_utxo)
            ci_good = true;

        if (!ci_good) {
            return false;
        }

        if (access->group->entry.out.size() == 0) {
            // Convention here: run on zero-out-edges so that we can collect results still

            chain_info_t chain_info = pack_chain_info(DOGE_KEY, DISTANCE_KEY, 0);
            auto b_key = access->key.b;
            auto c_key = access->key.c;
            dbkey_t search_key  = {chain_info, b_key, c_key};

            char* ret_val = nullptr;
            access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &ret_val);
            if (ret_val == nullptr) {
                // result entry did not exist, so we run
                return true;
            } else {
                // result entry already existed, no need to run again
                return false;
            }
        }

        // If any of the entries don't have the filter_done tag, we need to re-run
        for (auto & edge : access->group->entry.out) {
            auto tags = edge.acc->tags;
            bool has_ran = false;
            while ((*tags)[0] != '\0') {
                string s (*tags);
                if (string(*tags) == filter_done_tag) has_ran = true;
                ++tags;
            }

            // If any edge has not run, return true right away
            if (!has_ran) {
                return true;
            }
        }

        // All edges have run
        return false;
    }

    /** @brief Contains the entry point and tags for the filter */
    extern const FilterInterface filter {
        filter_name: filter_name,
        filter_type: GROUP_ENTRIES,
        should_run: &should_run,
        init: &init,
        destroy: &destroy,
        run: &run
    };
}

