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

bool should_run_btc_based_tx_vals(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag, const char* inactive_tag) {
    auto tags = access->tags;
    bool found_crypto = false;
    bool found_tx = false;

    //make sure the entry includes "BTC" and "tx" but not
    //"BTC_tx_out_edges:done" or "BTC_tx_out_edges:fail"
    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
            found_crypto = true;
        if (string(*tags) == "tx")
            found_tx = true;
        if (string(*tags) == inactive_tag)
            return false;
        if (string(*tags) == filter_done_tag)
            return false;
        if (string(*tags) == filter_fail_tag)
            return false;
        ++tags;
    }
    if (found_crypto && found_tx)
        return true;
    return false;
}

void btc_based_tx_vals(const DBAccess *access, const char* crypto_tag, const char* filter_name, const char* filter_done_tag, const char* filter_fail_tag, const char* inactive_tag) {
   const uint32_t crypto_id = get_blockchain_key(crypto_tag);

   // Parse the tx a bit further
   Document d;
   d.Parse(access->value);

   if (d.HasParseError()) return fail_(access, filter_name, filter_fail_tag, "cannot parse");

   // find the transactions in the json
   if (!d.HasMember("tx")) return fail_(access, filter_name, filter_fail_tag, "no member tx");
   auto &tx = d["tx"];
   if (!tx.IsObject()) return fail_(access, filter_name, filter_fail_tag, "tx is not an object");
   if (!tx.HasMember("txid")) return fail_(access, filter_name, filter_fail_tag, "txid not found");
   auto &txid = tx["txid"];
   if (!txid.IsString()) return fail_(access, filter_name, filter_fail_tag, "txid is not a string");
   string txid_s;
   txid_s.assign(txid.GetString());

   vtx_t tx_hash_key;
   stringstream tx_hash_ss;
   tx_hash_ss << hex << txid_s.substr(0, 15);
   tx_hash_ss >> tx_hash_key;

   // Lookup the time of the transaction
   chain_info_t chain_info = pack_chain_info(crypto_id, TXTIME_KEY, 0);
   vtx_t src_key;
   stringstream ss;
   ss << txid_s.substr(0, 15);
   ss >> hex >> src_key;
   dbkey_t time_lookup_key { chain_info, src_key, 0 };

   bool inactive = false;
   bool timestamp_found = false;

   char* time_ret = nullptr;
   access->get_entry_by_key.run(&access->get_entry_by_key, time_lookup_key, &time_ret);
   string time_ret_s;
   time_t timestamp = 0;
   if (time_ret != nullptr) {
      time_ret_s.assign(time_ret);
      free(time_ret);
      timestamp = stoi(time_ret_s);
      timestamp_found = true;
   } else {
      access->add_tag.run(&access->add_tag, inactive_tag);
      access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, time_lookup_key, inactive_tag);
      inactive = true;
   }

   // For each vout value, create a new entry for it
   if (!tx.HasMember("vout")) return fail_(access, filter_name, filter_fail_tag, "tx had no member \"vouts\"");
   auto &vouts = tx["vout"];
   if (!vouts.IsArray()) return fail_(access, filter_name, filter_fail_tag, "\"vouts\" was not an array");

   for (rapidjson::Value::ConstValueIterator itr = vouts.Begin(); itr != vouts.End(); ++itr) {
       // Create an entry for the out value in BTC
       const rapidjson::Value& vout = *itr;
       if (!vout.HasMember("value")) return fail_(access, filter_name, filter_fail_tag, "vout had no member \"value\"");
       auto val = vout["value"].GetFloat();
       string val_s = to_string(val);
       string lower_crypto_name = string(crypto_tag);
       for (auto& x : lower_crypto_name) {
           x = tolower(x);
       }

       if (!vout.HasMember("n")) return fail_(access, filter_name, filter_fail_tag, "vout has no member \"n\"");
       auto &n_raw = vout["n"];
       if (!n_raw.IsInt()) return fail_(access, filter_name, filter_fail_tag, "'n' was not an integer");
       uint16_t n = n_raw.GetInt();
 
       string crypto_out_tag = "out_val_" + lower_crypto_name;
       const char* new_tags[] = {crypto_tag, crypto_out_tag.c_str(), ""};
       chain_info_t ci = pack_chain_info(crypto_id, OUT_VAL_KEY, n);
       dbkey_t new_key = {ci, tx_hash_key, 0};
       access->make_new_entry.run(&access->make_new_entry, new_tags, val_s.c_str(), new_key);
       
       // Create the equivalent entry converted to USD
       if (!timestamp_found) continue;
       float exchange_rate = get_exchange_rate(timestamp, access, inactive_tag, crypto_id);
       if (exchange_rate < 0) {
           inactive = true;
           continue;
       }
       vtx_t b_key = timestamp;
       const char* new_tags_usd[] = {crypto_tag, "out_val_usd", ""};
       chain_info_t ci_usd = pack_chain_info(USD_KEY, OUT_VAL_KEY, n);
       dbkey_t new_key_usd = {ci_usd, b_key, tx_hash_key};
       string val_usd_s = to_string(val* exchange_rate);
       access->make_new_entry.run(&access->make_new_entry, new_tags_usd, val_usd_s.c_str(), new_key_usd);
   } 

   // Add a tag indicating parsing was successful
   if (!inactive)
       access->add_tag.run(&access->add_tag, filter_done_tag);
}




