#include <iostream>
#include <exception>
#include <string>

#include "address.hpp"
#include "par_db.hpp"

#include "util.hpp"

using namespace std;

using namespace pando;

volatile sig_atomic_t *g_shutdown = nullptr;

void si_handle(int) {
    cerr << "[Pando] Ctrl+C caught, shutting down..." << endl;
    if (g_shutdown == nullptr) throw runtime_error("Unable to find shutdown flag");
    *g_shutdown = true;
}
void set_handler() {
    struct sigaction si_handler;
    si_handler.sa_handler = si_handle;
    sigemptyset(&si_handler.sa_mask);
    si_handler.sa_flags = SA_RESTART;
    sigaction(SIGINT, &si_handler, NULL);
}

int main_(int argc, char **argv) {
    cerr << "[Pando] [INFO] Loading..." << endl;

    if (argc < 2 || argc > 5) {
        cerr << "Usage: pando_pardb bind-addr [seed-addr] [-M<mem in GB>]\n"
            "\n"
            "Parameters:\n"
            "  bind-addr : the address to bind this specific DB agent to\n"
            "  seed-addr : an address in a mesh to join\n"
            "  -M<mem> : memory in GB, defaults to 16 (e.g., -M8 would allocate 8 GB)\n"
            "  --skip-group-filters : skip processing of group filters\n"
            "\n"
            "Addresses are of the form: IPv4-string,ID\n"
            "  IPv4-string : a period separated IP address, e.g., 1.2.3.4\n"
            "  ID : a small integer between 0 and 199\n"
            "Addresses should be globally unique." << endl;
        return 1;
    }

    elga::ZMQAddress bind_addr = get_zmq_addr(argv[1]);
    string seed_addr;
    size_t sz = 16ull*(1ull<<30);
    bool skip_group_filters = false;
    for (int idx = 2; idx < argc; ++idx) {
        if (argv[idx][0] == '-' && argv[idx][1] == 'M') {
            sz = (1ull<<30)*strtoul(&(argv[idx][2]), NULL, 10);
        } else if (std::string(argv[idx]) == "--skip-group-filters") {
            skip_group_filters = true;
        } else {
            if (seed_addr.size() != 0) throw runtime_error("Multiple seed addrs given");
            seed_addr.assign(argv[idx]);
        }
    }

    cerr << "[Pando] [DEBUG] Bind addr=" << bind_addr.get_conn_str(bind_addr, REQUEST) << " memory=" << sz << endl;
    ParDBThread db { bind_addr, sz, skip_group_filters };

    if (argc > 2) {
        elga::ZMQAddress seed_addr = get_zmq_addr(argv[2]);
        if (seed_addr.serialize() != bind_addr.serialize()) {
            // Build a client and request to add the neighbor
            cerr << "[Pando] [INFO] Connecting to neighbor..." << endl;
            cerr << "[Pando] [DEBUG] Neighbor addr=" << seed_addr.get_conn_str(bind_addr, REQUEST) << endl;
            ParDBClient c { bind_addr };
            c.add_neighbor(seed_addr);
        }
    }

    // Setup the handler
    g_shutdown = db.get_shutdown_flag();
    set_handler();

    cerr << "[Pando] [INFO] Running" << endl;
    db.join();

    return 0;
}

int main(int argc, char **argv) {
    elga::ZMQChatterbox::Setup();

    int ret;

    try {
        ret = main_(argc, argv);
    } catch(...) {
        elga::ZMQChatterbox::Teardown();
        throw;
    }

    elga::ZMQChatterbox::Teardown();

    return ret;
}
