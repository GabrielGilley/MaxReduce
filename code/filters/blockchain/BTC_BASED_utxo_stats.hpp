#include "helpers.hpp"
#include <sstream>

using namespace std;
using namespace pando;
using namespace rapidjson;

template<typename ...Args>
void fail_(const DBAccess *access, const char* filter_name, const char* filter_fail_tag, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

bool should_run_btc_based_utxo_stats(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag) {
    const uint32_t crypto_id = get_blockchain_key(crypto_tag);

    uint32_t chain;
    uint16_t filter_1;
    unpack_chain_info(access->key.a, &chain, &filter_1, nullptr);

    // Ensure the blockchain is correct
    if (chain != crypto_id) return false;

    // Ensure the first key type for UTXO graph edges
    if (filter_1 != UTXO_EDGE) return false;

    // Do not process on entries with no out degree
    auto& group_access = access->group;
    if (group_access->entry.out.size() == 0) return false;

    // Do not process if all edges have already been processed
    bool all_processed = true;
    for (auto &out_edge : group_access->entry.out) {
        auto tags = out_edge.acc->tags;
        bool entry_processed = false;
        while ((*tags)[0] != '\0') {
            if (string(*tags) == filter_done_tag) {
               entry_processed = true;
                break;
            } else if (string(*tags) == filter_fail_tag) {
                entry_processed = true;
                break;
            }
            ++tags;
        }
        all_processed &= entry_processed;
    }
    if (all_processed) return false;

    return true;
}

void btc_based_utxo_stats(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag) {
    const uint32_t crypto_id = get_blockchain_key(crypto_tag);

    // Get the group access
    GroupAccess* group_access = access->group;
    
    string month_tag;
    month_tag.assign(to_string(group_access->vtx));
    month_tag = "month=" + month_tag;

    //output some statistics to an entry
    //key will be {BTC | UTXO_STATS | 0, month_ts, 0}
    //value should contain some way to format statistics
    const char* const new_tags[] = {crypto_tag, "utxo_stats", month_tag.c_str(), ""};
    dbkey_t new_key;
    new_key.a = pack_chain_info(crypto_id, UTXO_STATS, 0);
    new_key.b = group_access->vtx;
    new_key.c = 0;
    string new_value_str = month_tag + "\n" + "num_utxos=" + to_string(group_access->entry.out.size());
    access->make_new_entry.run(&access->make_new_entry, new_tags, new_value_str.c_str(), new_key);

    group_access->state = INACTIVE;

    for (auto &out_edge: group_access->entry.out) {
        auto out_edge_acc = out_edge.acc;
        out_edge_acc->add_tag.run(&out_edge_acc->add_tag, filter_done_tag);
    }
}

