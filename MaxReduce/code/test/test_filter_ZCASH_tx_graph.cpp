#include "seq_db.hpp"

#include "test.hpp"

using namespace std;
using namespace pando;

TEST(zcash_tx_graph) {
    SeqDB db { data_dir+"/zcash_blocks/zcash_blocks_small.txt" };
    
    db.add_filter_dir(build_dir);
    db.install_filter("ZEC_block_to_tx");
    db.process();
    db.install_filter("ZEC_tx_in_edges");
    db.process();
    db.install_filter("ZEC_tx_out_edges");
    db.process();

    //Check a few of the edges to ensure everything seems correct. Checking all
    //would be unreasonable.
    size_t num_in_edges = 0;
    size_t num_coinbase_in_edges = 0;
    size_t num_non_coinbase_in_edges = 0;

    size_t num_out_edges = 0;
    size_t num_utxo = 0;
    size_t num_non_utxo = 0;

    bool in_edge_314_found = false;
    string edge_314_tag = "from=31455d67498d00ca5327776bf060551b9f90110bacce4984fa57ed213f1a140a";

    bool in_edge_1db_found = false;
    string edge_1db_tag = "from=1db24e115e350a0676ea65e97564d87b56206e31330980ad3d6f50ef8168ed12";

    bool out_edge_0fa_found = false;
    string edge_0fa_tag = "to=0fae8554ede80cd301247db6f4fe6f058e1649a8e730a7e2444dedbc91c09550";

    bool out_edge_ec3_found = false;
    string edge_ec3_tag = "to=ec31a1b3e18533702c74a67d91c49d622717bd53d6192c5cb23b9bdf080416a5";

    for (auto & [key, entry] : db.entries()) {
        if (entry.has_tag("tx-in-edge")) {
            num_in_edges++;
            if (entry.has_tag("from=COINBASE"))
                num_coinbase_in_edges++;
            else
                num_non_coinbase_in_edges++;

            if (entry.has_tag(edge_314_tag)) {
                if (in_edge_314_found) TEST_FAIL;
                in_edge_314_found = true;
            }
            if (entry.has_tag(edge_1db_tag)) {
                if (in_edge_1db_found) TEST_FAIL;
                in_edge_1db_found = true;
            }
        }
        if (entry.has_tag("tx-out-edge")) {
            num_out_edges++;
            if (entry.has_tag("to=UTXO"))
                num_utxo++;
            else
                num_non_utxo++;
            if (entry.has_tag(edge_0fa_tag))
                out_edge_0fa_found = true;
            if (entry.has_tag(edge_ec3_tag))
                out_edge_ec3_found = true;
        }
    }

    size_t expected_in_edges = 210;
    EQ(num_in_edges, expected_in_edges);
    EQ(num_coinbase_in_edges, 208);
    EQ(num_non_coinbase_in_edges, 2);
    EQ(in_edge_314_found, true);
    EQ(in_edge_1db_found, true);

    size_t expected_out_edges = 419;
    EQ(num_out_edges, expected_out_edges);
    EQ(num_utxo, 417);
    EQ(num_non_utxo, 2);
    EQ(out_edge_0fa_found, true);
    EQ(out_edge_ec3_found, true);

    TEST_PASS
}

TESTS_BEGIN
    elga::ZMQChatterbox::Setup();
    RUN_TEST(zcash_tx_graph)
    elga::ZMQChatterbox::Teardown();
TESTS_END
