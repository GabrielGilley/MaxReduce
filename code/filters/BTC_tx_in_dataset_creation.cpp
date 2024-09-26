#include <iostream>

#include "filter.hpp"
#include "dbkey.h"
#include "rapidjson/document.h"
#include "helpers.hpp"

#include <string>
#include <sstream>
#include <iomanip>

#include <boost/algorithm/string.hpp>

using namespace std;
using namespace pando;
using namespace rapidjson;

#define FILTER_NAME "BTC_tx_in_dataset_creation"
static const char* filter_name = FILTER_NAME;
static const char* filter_done_tag = FILTER_NAME ":done";
static const char* filter_fail_tag = FILTER_NAME ":fail";
static const char* filter_inactive_tag = FILTER_NAME ":inactive";
static const char* filter_first_run_tag = FILTER_NAME ":first_run_complete";


template<typename ...Args>
void fail_(const DBAccess *access, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

bool is_coinbase(const DBAccess *access) {
    auto tags = access->tags;

    while ((*tags)[0] != '\0') {
        if (string(*tags) == "from=COINBASE")
            return true;
        ++tags;
    }
    return false;
}

void run_(const DBAccess *access) {
    // define tags for all entry types that will be created
    const char* new_tags_addr_to_input[] = {"BTC", "address_to_tx_inputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_input[] = {"BTC", "tx_inputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx_to_input[] = {"BTC", "tx_to_inputs.csv", "CROSSCHAIN_DATASET", ""};

    // check if we have the first run complete tag. If so, it means we've already ran
    // once, and some of these entries have been created. Since we are using
    // the INITIAL_KEY instead of setting one specifically, we need to not
    // create the entries again if this case occurs
    bool create_entries = !has_tag(access, filter_first_run_tag);

    string txid = access->value;
    stringstream ss;
    ss << txid.substr(0,15);
    vtx_t txid_key;
    ss >> hex >> txid_key;

    dbkey_t input_id_key = access->key;
    stringstream input_id_ss;
    input_id_ss << input_id_key.a << input_id_key.b << input_id_key.c;
    string input_id = input_id_ss.str();

    // lookup the value of this input
    string val_s;
    if (is_coinbase(access)) {
        // Since it's a coinbase, we need to query the input value differently.
        // It's not easy to get, so we'll just query the full transaction and sum the vouts
        chain_info_t search_ci = pack_chain_info(BTC_KEY, TX_KEY, 0);
        dbkey_t search_key {search_ci, txid_key, 0};
        char* val_ret = nullptr;

        string full_tx;
        access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &val_ret);
        if (val_ret != nullptr) {
            full_tx.assign(val_ret);
            free(val_ret);
            
            // sum the vouts
           Document tx_doc;
           tx_doc.Parse(full_tx.c_str());
           if (tx_doc.HasParseError()) return fail_(access, filter_fail_tag, filter_name, "parse error");
           if (!tx_doc.HasMember("tx")) return fail_(access, filter_name, filter_fail_tag, "had no member \"tx\"");
           auto &tx = tx_doc["tx"];
           if (!tx.HasMember("vout")) return fail_(access, filter_name, filter_fail_tag, "tx had no member \"vout\"");
           auto &vouts = tx["vout"];
           if (!vouts.IsArray()) return fail_(access, filter_name, filter_fail_tag, "\"vouts\" was not an array");
           double vout_sum = 0;
           for (rapidjson::Value::ConstValueIterator itr = vouts.Begin(); itr != vouts.End(); ++itr) {
               const rapidjson::Value& vout = *itr;
               if (!vout.HasMember("value")) return fail_(access, filter_name, filter_fail_tag, "vout had no member \"value\"");
               auto val = vout["value"].GetFloat();
               vout_sum += val;
           }

           val_s = to_string(vout_sum);


        } else {
            access->add_tag.run(&access->add_tag, filter_inactive_tag);
            access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key, filter_inactive_tag);
            return;
        }
    } else {
        chain_info_t search_ci = pack_chain_info(BTC_KEY, OUT_VAL_KEY, access->key.c);
        dbkey_t search_key { search_ci, access->key.b, 0 };
        char* val_ret = nullptr;
        access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &val_ret);
        if (val_ret != nullptr) {
            val_s.assign(val_ret);
            free(val_ret);
        } else {
            access->add_tag.run(&access->add_tag, filter_inactive_tag);
            access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key, filter_inactive_tag);
            return;
        }
    }

    
    // make the tx_input entry
    string input_val = input_id + "," + val_s;
    if (create_entries)
        access->make_new_entry.run(&access->make_new_entry, new_tags_input, input_val.c_str(), INITIAL_KEY);

    // Next, make the tx_to_input entry
    stringstream tx_to_input_val_ss;
    tx_to_input_val_ss << access->value << "," << input_id;
    string tx_to_input_val = tx_to_input_val_ss.str();
    if (create_entries)
        access->make_new_entry.run(&access->make_new_entry, new_tags_tx_to_input, tx_to_input_val.c_str(), INITIAL_KEY);

    // add a tag indicating that the first run has completed. That way, if the
    // following lookup fails and the filter subsequently has to run again, the
    // entries don't get created again
    access->add_tag.run(&access->add_tag, filter_first_run_tag);

    // Now, make the address_to_tx_input entry
    // expect an entry with key {{BTC_KEY, VOUT_ADDR_KEY, 0}, txid, n} and value is the address (if there is one -- if not, the entry should not exist)
    // lookup the address of this input (if there is one)
    chain_info_t search_addr_ci = pack_chain_info(BTC_KEY, VOUT_ADDR_KEY, 0);
    dbkey_t search_key_addr { search_addr_ci, txid_key, access->key.c };
    char* addr_ret = nullptr;
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key_addr, &addr_ret);
    string addr_s;
    if (addr_ret != nullptr) {
        addr_s.assign(addr_ret);
        free(addr_ret);
    } else {
        access->add_tag.run(&access->add_tag, filter_inactive_tag);
        access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key_addr, filter_inactive_tag);
        return;
    }

    vector<string> vout_addrs;
    boost::split(vout_addrs, addr_s, boost::is_any_of("\n"));

    for (auto & vout_addr : vout_addrs) {
        string new_val_addr = vout_addr + "," + input_id;
        access->make_new_entry.run(&access->make_new_entry, new_tags_addr_to_input, new_val_addr.c_str(), INITIAL_KEY);
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
        bool found_edge = false;

        while ((*tags)[0] != '\0') {
            if (string(*tags) == "BTC")
                found_btc = true;
            if (string(*tags) == "tx-in-edge")
                found_edge = true;
            if (string(*tags) == filter_done_tag)
                return false;
            if (string(*tags) == filter_fail_tag)
                return false;
            if (string(*tags) == filter_inactive_tag)
                return false;
            ++tags;
        }
        if (found_btc && found_edge)
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

