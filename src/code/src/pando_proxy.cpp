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

    if (argc != 3) {
        cerr << "Usage: pando_proxy bind-addr seed-addr\n"
            "\n"
            "Parameters:\n"
            "  bind-addr : the address to bind this specific DB agent to\n"
            "  seed-addr : an address in a mesh to join\n"
            "\n"
            "Addresses are of the form: IPv4-string,ID\n"
            "  IPv4-string : a period separated IP address, e.g., 1.2.3.4\n"
            "  ID : a small integer between 0 and 199\n"
            "Addresses should be globally unique." << endl;
        return 1;
    }

    elga::ZMQAddress bind_addr = get_zmq_addr(argv[1]);
    elga::ZMQAddress seed_addr = get_zmq_addr(argv[2]);
    if (seed_addr.serialize() == bind_addr.serialize()) throw runtime_error("Unable to connect proxy to itself");

    cerr << "[Pando] [DEBUG] Bind addr=" << bind_addr.get_conn_str(bind_addr, REQUEST) << endl;
    ParDBThread<PandoProxy> db { bind_addr };

    // Build a client and request to add the neighbor
    cerr << "[Pando] [INFO] Connecting to neighbor..." << endl;
    cerr << "[Pando] [DEBUG] Neighbor addr=" << seed_addr.get_conn_str(bind_addr, REQUEST) << endl;
    ParDBClient c { bind_addr };
    c.add_neighbor(seed_addr);

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
