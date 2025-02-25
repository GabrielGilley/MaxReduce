#include <iostream>
#include <exception>
#include <string>
#include <iomanip>
#include <sstream>

#include <sys/ioctl.h>

#include <algorithm>
#include <cctype>
#include <regex>

#include "address.hpp"
#include "par_db.hpp"

#include "util.hpp"

using namespace std;
using namespace pando;

inline size_t get_db_size(addr_t a) {
    elga::ZMQAddress empty;
    elga::ZMQAddress addr { a };
    elga::ZMQRequester r { addr, empty };
    r.send(DB_SIZE);
    ZMQMessage resp = r.read();
    const char *resp_data = resp.data();
    size_t ret = unpack_single<size_t>(resp_data);
    return ret;
}

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

/** From https://stackoverflow.com/questions/68639888/determine-the-print-size-of-a-string-containing-escape-characters */
regex ansi_reg("\033((\\[((\\d+;)*\\d+)?[A-DHJKMRcf-ilmnprsu])|\\(|\\))");
string::iterator::difference_type count_no_escape(string const& str) {
    string::iterator::difference_type result = 0;
    for_each(sregex_token_iterator(str.begin(), str.end(), ansi_reg, -1),
            sregex_token_iterator(), [&result](sregex_token_iterator::value_type const& e) {
            string tmp(e);
            result += count_if(tmp.begin(), tmp.end(), ::isprint);
            });
    return result;
}

template<typename ...Args>
inline void pline(int& r, int c, Args && ...args) {
    cout << "\e[0m║ ";
    int pos = 2;

    stringstream ss;

    (ss << ... << args);

    string l = ss.str();
    cout << l;

    pos += count_no_escape(l);
    cout << "\e[" << c-pos-1 << "C";

    cout << "║\n";
    cout << "\e[0m";
    ++r;
}

inline void pmid(int& r, int c) {
    cout << "\e[0m╟";
    int pos = 1;
    for (; pos < c-1; ++pos) cout << "─";
    cout << "╢\n";
    cout << "\e[0m";
    ++r;
}

void top(PandoParticipant& pp) {
    volatile sig_atomic_t shutdown = false;
    g_shutdown = &shutdown;
    set_handler();

    size_t status = 0;

    int old_c = 0;
    int old_r = 0;
    size_t old_as = 0;

    while (!shutdown) {
        this_thread::sleep_for(chrono::milliseconds(1500));
        while (pp.recv_msg()) { }

        // Retrieve information from all agents
        auto& agents = pp.agents();
        vector<size_t> db_sizes;
        db_sizes.reserve(agents.size());
        size_t total_db_size = 0;
        for (addr_t a : agents) {
            size_t a_size = get_db_size(a);
            db_sizes.push_back(a_size);
            total_db_size += a_size;
        }

        // Now do the update

        // Get the terminal information
        struct winsize w;
        ioctl(0, TIOCGWINSZ, &w);

        int r = 1;
        int max_r = w.ws_row;
        int c = w.ws_col;

        if (c < 10) {
            cout << "Screen too small" << endl;
            continue;
        }

        if (agents.size() != old_as || max_r != old_r || c != old_c) {
            cout << "\e[2J\e[1;1H\e[0m";
            old_r = max_r;
            old_c = c;
            old_as = agents.size();
        }

        // Begin drawing
        cout << "\e[H\e[0m";
        cout << "╔";
        for (int pos = 1; pos < w.ws_col-1; ++pos) {
            cout << "═";
        }
        cout << "╗" << endl;
        ++r;

        char status_char;
        if (status % 4 == 0)
            status_char = '-';
        else if (status % 4 == 1)
            status_char = '\\';
        else if (status % 4 == 2)
            status_char = '|';
        else if (status % 4 == 3)
            status_char = '/';
        ++status;

        pline(r, c, "PANDO ParDB Monitor ", status_char);
        pmid(r, c);

        pline(r, c, "Mesh size     : \e[32m", setw(8), agents.size());
        pline(r, c, "Total DB size : \e[32m", setw(8), total_db_size);

        pmid(r, c);

        // Now, show information about each agent
        // These are in blocks

        const int blk_width = 10+2;
        const int blk_height = 3;
        const int agents_per_line = (c-4)/blk_width;

        size_t agents_rem = agents.size();

        while (agents_rem > 0) {
            // Create a new line of agents
            for (int p = 0; p < blk_height; ++p)
                cout << "║\e[B\e[D";
            for (int p = 0; p < blk_height; ++p)
                cout << "\e[A";
            cout << "\e[C ";

            int agents_this_line = 0;
            while (agents_rem > 0 && agents_this_line < agents_per_line) {
                // Print out the agent block
                // [ size | last HB ] 
                cout << ' ';
                cout << "┌────────┐";
                cout << "\e[" << blk_width-2 << "D\e[B";
                cout << "│" << setw(blk_width-4) << db_sizes[agents_rem-1] << "│";
                cout << "\e[" << blk_width-2 << "D\e[B";
                cout << "└────────┘";
                cout << "\e[" << blk_width-2 << "D\e[B";

                // Move to the start of the next block
                cout << "\e[" << blk_height << "A\e[" << blk_width-1 << "C";

                --agents_rem;
                ++agents_this_line;
            }

            // Move to the end
            for (int p = 2+blk_width*agents_this_line; p < c-1; ++p)
                cout << ' ';

            for (int p = 0; p < blk_height; ++p)
                cout << "║\e[B";
            cout << "\e[G";

            r += blk_height;
        }

        // Move to the end
        while (r < max_r-1) {
            pline(r, c, "");
        }

        cout << "╚";
        for (int pos = 1; pos < w.ws_col-1; ++pos) {
            cout << "═";
        }
        cout << "╝\e[0m" << endl;

    }
}

int main_(int argc, char **argv) {
    if (argc < 3) {
        cerr << "Usage: pando db-addr comand [args...]\n"
            "\n"
            "Parameters:\n"
            "  mesh-addr : an address in the mesh to join\n"
            "  command   : the command to run, with appropriate args\n"
            "\n"
            "Addresses are of the form: IPv4-string,ID\n"
            "  IPv4-string : a period separated IP address, e.g., 1.2.3.4\n"
            "  ID : a small integer between 0 and 199\n"
            "\n"
            "Available commands are:\n"
            "  total_db_size : return the size of the full DB\n"
            "  top           : return information on the mesh\n"
            ;
        return 1;
    }

    elga::ZMQAddress mesh_addr = get_zmq_addr(argv[1]);
    string cmd { argv[2] };

    elga::ZMQAddress empty;

    PandoParticipant pp { empty, false };
    pp.add_neighbor(mesh_addr.serialize(), false);
    pp.request_mesh(mesh_addr.serialize());

    if (cmd == "top") {
        top(pp);
        return 0;
    }

    cerr << "Initializing..." << endl;

    // Now, wait for the mesh to be stable
    pp.recv_msg();
    size_t p_size = pp.agents().size();
    while (true) {
        pp.recv_msg();
        if (pp.agents().size() == p_size) break;
        p_size = pp.agents().size();
    }

    if (cmd == "total_db_size") {
        // Request the size from the whole mesh
        size_t total_db_size = 0;
        for (addr_t a : pp.agents()) {
            size_t ret = get_db_size(a);
            total_db_size += ret;
            cerr << "[" << setw(11) << a << "]\t" << ret << endl;
        }
        cout << total_db_size << endl;

        return 0;
    }

    throw runtime_error("Unknown command");

    return 1;
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

