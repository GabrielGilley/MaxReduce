#include <iostream>

#include "filter.hpp"
#include "dbkey.h"

#include <string>
#include <sstream>
#include <iomanip>

#include "rapidjson/document.h"

using namespace std;
using namespace pando;
using namespace rapidjson;

#define FILTER_NAME "BTC_tx_out_dataset_creation"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";

template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void run_(const DBAccess *access) {
    // define tags for all entry types that will be created
    const char* new_tags_addr_to_output[] = {"BTC", "address_to_tx_outputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_output[] = {"BTC", "tx_outputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx_to_output[] = {"BTC", "tx_to_outputs.csv", "CROSSCHAIN_DATASET", ""};

    Document d;
    d.Parse(access->value);

    if (d.HasParseError()) return fail_(access, "cannot parse");

    // Find the transactions in the block
    if (!d.HasMember("tx")) return fail_(access, "no member tx");
    auto &tx = d["tx"];
    if (!tx.IsObject()) return fail_(access, "tx is not an object");

    if (!tx.HasMember("txid")) return fail_(access, "txid not found");
    auto &txid_raw = tx["txid"];
    if (!txid_raw.IsString()) return fail_(access, "txid is not a string");
    string txid = txid_raw.GetString();
    stringstream ss;
    ss << txid.substr(0,15);
    vtx_t txid_key;
    ss >> hex >> txid_key;

    if (!tx.HasMember("vout")) return fail_(access, "no vout object");
    auto &vouts = tx["vout"];
    if (!vouts.IsArray()) return fail_(access, "vout is not an array");

    for (auto & vout: vouts.GetArray()) {
        // Find the position
        if (!vout.HasMember("n")) return fail_(access, "no 'n' in vout");
        if (!vout["n"].IsInt()) return fail_(access, "'n' is not an int");
        int n = vout["n"].GetInt();
        vtx_t n_key = n;

        // Get the transaction value
        if (!vout.HasMember("value")) return fail_(access, "no value found in vout");
        if (!vout["value"].IsDouble()) return fail_(access, "value is not an int");
        double value = vout["value"].GetDouble();

        // slightly unintuitive to use TX_IN_EDGE_KEY here, but we want to match IDs with in-edges
        chain_info_t ci = pack_chain_info(BTC_KEY, TX_IN_EDGE_KEY, 0);
        dbkey_t new_key {ci, n_key, txid_key};

        stringstream tx_output_id_ss;
        tx_output_id_ss << new_key.a << new_key.b << new_key.c;
        string tx_output_id = tx_output_id_ss.str();

        stringstream new_val_ss;
        new_val_ss << tx_output_id << "," << value;
        string new_value = new_val_ss.str();

        // Use INITIAL_KEY here. the "new_key" we created was just to match the tx-in-edge, but with a different value
        access->make_new_entry.run(&access->make_new_entry, new_tags_output, new_value.c_str(), INITIAL_KEY);

        // Next, use information we already have to create the tx_to_output entry
        stringstream tx_to_o_ss;
        tx_to_o_ss << txid << "," << tx_output_id;
        string new_val_tx_to_o = tx_to_o_ss.str();

        access->make_new_entry.run(&access->make_new_entry, new_tags_tx_to_output, new_val_tx_to_o.c_str(), INITIAL_KEY);

        // Now, pull out address information to link an address to this tx_output
        // REMEMBER! Not all transactions have associated addresses, that's fine
        if (!vout.HasMember("scriptPubKey")) continue;
        auto &scriptPubKey = vout["scriptPubKey"];
        if (!scriptPubKey.IsObject()) return fail_(access, "\"scriptPubKey\" was not an object");
        if (!scriptPubKey.HasMember("addresses")) continue;
        auto &addrs = scriptPubKey["addresses"];
        if (!addrs.IsArray()) return fail_(access, "\"addresses\" was not an array");
        for (auto & addr : addrs.GetArray()) {
            if (!addr.IsString()) return fail_(access, "address was not a string");
            stringstream addr_val_ss;
            addr_val_ss << addr.GetString() << "," << tx_output_id;
            string addr_val = addr_val_ss.str();
            access->make_new_entry.run(&access->make_new_entry, new_tags_addr_to_output, addr_val.c_str(), INITIAL_KEY);

        }
    }


    // Add a tag indicating parsing was successful
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
        bool found_btc = false;
        bool found_tx = false;

        while ((*tags)[0] != '\0') {
            if (string(*tags) == "BTC")
                found_btc = true;
            if (string(*tags) == "tx")
                found_tx = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            ++tags;
        }
        if (found_btc && found_tx)
            return true;
        return false;
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

