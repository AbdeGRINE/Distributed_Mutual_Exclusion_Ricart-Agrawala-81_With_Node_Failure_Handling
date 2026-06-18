/**
 * ra81_process.cpp
 * Simulates one peer process of Ricart & Agrawala 81 mutual exclusion.
 *
 * Usage:  ./ra81_process <id>    (id ∈ [1..10])
 *
 * Each process:
 *   - Listens on TCP port (BASE_PORT + id) for incoming messages
 *   - Runs the RA81 algorithm with Lamport logical clocks
 *   - Loops forever: random sleep → request CS → CS work → release
 *   - Writes a JSON status file to /tmp/ra81_proc_<id>.json after every
 *     state change so the GUI can poll it.
 *
 * Fault injection: send SIGUSR1 to a process to toggle FAILED state.
 *   When FAILED, it ignores all incoming REQ and does not request CS itself.
 *   Send SIGUSR1 again to recover.
 *
 * ────────────────────────────────────────────────────────────────────────
 * CORRECTIFS APPLIQUÉS (gestion des pannes) :
 *
 *   [FIX 1] sig_handler() : au moment de passer en FAILED, on envoie
 *           immédiatement les REP différées qu'on devait à d'autres
 *           processus (g_deferred[]), au lieu de les laisser bloqués
 *           pour toujours. L'envoi se fait dans un thread détaché car
 *           send_rep()/send_msg() sont bloquants (réseau) et ne doivent
 *           pas s'exécuter dans le contexte du signal handler lui-même.
 *
 *   [FIX 2] request_cs() : ajout d'un timeout (REQUEST_TIMEOUT_MS) sur
 *           l'attente des REP. Si un peer est FAILED (ou injoignable) et
 *           ne répond jamais, on n'attend plus indéfiniment : on abandonne
 *           la tentative de SC en cours, on réinitialise notre état, et
 *           on retentera au prochain cycle de la boucle principale.
 *           request_cs() retourne désormais un bool (entré en CS ou non),
 *           et main() doit sauter la section critique si la valeur est
 *           false (sinon on entrerait en CS sans avoir reçu toutes les
 *           REP, ce qui casserait l'exclusion mutuelle).
 * ────────────────────────────────────────────────────────────────────────
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <array>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <random>
#include <csignal>
#include <cassert>
#include <algorithm>

// POSIX / sockets
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>

#include "ra81_common.h"

// ────────────────────────────────────────────────────────────────────────────
// Global process state
// ────────────────────────────────────────────────────────────────────────────
static int MY_ID = -1;

// [FIX 2] Délai maximal d'attente des REP avant d'abandonner une demande de
// SC. Doit être nettement supérieur au timeout TCP de send_msg() (1000ms)
// multiplié par le nombre de peers potentiellement injoignables.
static constexpr int REQUEST_TIMEOUT_MS = 4000;

// RA81 variables (protected by g_mutex unless noted as atomic)
static std::mutex g_mutex;
static std::condition_variable g_cv;

static int64_t  g_clock      = 0;         // Lamport logical clock
static int64_t  g_osn        = 0;         // our sequence number (estampille)
static int      g_replies_needed = 0;     // count of pending REP messages
static bool     g_sc_wanted  = false;     // we want to enter CS
static bool     g_in_cs      = false;     // we are inside CS
static std::array<bool, N+1> g_deferred = {}; // g_deferred[j] = we owe REP to j
static ProcState g_state = ProcState::IDLE;

// Fault simulation
static std::atomic<bool> g_failed{false};

// Per-peer "last known req timestamp" for GUI display
static std::array<int64_t, N+1> g_peer_req_ts = {}; // last REQ stamp from peer j

// ────────────────────────────────────────────────────────────────────────────
// Logging / JSON state file
// ────────────────────────────────────────────────────────────────────────────
static void write_state_json() {
    // Called while holding g_mutex (or at startup)
    std::ostringstream ss;
    ss << "{\n";
    ss << "  \"id\": "      << MY_ID << ",\n";
    ss << "  \"state\": \"" << state_name(g_state) << "\",\n";
    ss << "  \"clock\": "   << g_clock << ",\n";
    ss << "  \"osn\": "     << g_osn << ",\n";
    ss << "  \"replies_needed\": " << g_replies_needed << ",\n";
    ss << "  \"sc_wanted\": " << (g_sc_wanted ? "true" : "false") << ",\n";
    ss << "  \"deferred\": [";
    bool first = true;
    for (int j = 1; j <= N; j++) {
        if (g_deferred[j]) {
            if (!first) ss << ",";
            ss << j;
            first = false;
        }
    }
    ss << "],\n";
    ss << "  \"peer_req_ts\": {";
    first = true;
    for (int j = 1; j <= N; j++) {
        if (j == MY_ID) continue;
        if (!first) ss << ",";
        ss << "\"" << j << "\":" << g_peer_req_ts[j];
        first = false;
    }
    ss << "},\n";
    ss << "  \"ts\": " << std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count() << "\n";
    ss << "}\n";

    std::ofstream f(state_file(MY_ID), std::ios::trunc);
    f << ss.str();
}

static void log(const std::string& msg) {
    auto now = std::chrono::system_clock::now();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                   now.time_since_epoch()).count() % 1000000;
    std::cout << "[P" << MY_ID << " t=" << ms << "] " << msg << "\n" << std::flush;
}

// ────────────────────────────────────────────────────────────────────────────
// Networking helpers
// ────────────────────────────────────────────────────────────────────────────
static bool send_msg(int dest_id, const Message& m) {
    // Create a new TCP connection each time (simple, avoids persistent conn mgmt)
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    // Short connect timeout via non-blocking trick
    struct timeval tv; tv.tv_sec = 1; tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(process_port(dest_id));
    inet_pton(AF_INET, HOST, &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return false;
    }

    // Serialize: just send raw struct (little-endian, localhost only)
    Message wire = m;
    wire.timestamp = htobe64(m.timestamp);
    wire.sender_id = htonl(m.sender_id);

    ssize_t sent = send(sock, &wire, sizeof(wire), MSG_NOSIGNAL);
    close(sock);
    return sent == (ssize_t)sizeof(wire);
}

static void broadcast_req(int64_t stamp) {
    Message m;
    m.type      = MsgType::REQ;
    m.sender_id = MY_ID;
    m.timestamp = stamp;
    memset(m.pad, 0, sizeof(m.pad));
    for (int j = 1; j <= N; j++) {
        if (j == MY_ID) continue;
        if (!send_msg(j, m))
            log("WARN: could not reach P" + std::to_string(j));
    }
}

static void send_rep(int dest_id) {
    Message m;
    m.type      = MsgType::REP;
    m.sender_id = MY_ID;
    {
        std::lock_guard<std::mutex> lk(g_mutex);
        m.timestamp = g_clock;
    }
    memset(m.pad, 0, sizeof(m.pad));
    send_msg(dest_id, m);
}

// ────────────────────────────────────────────────────────────────────────────
// Lamport clock helpers (call with g_mutex held)
// ────────────────────────────────────────────────────────────────────────────
static void clock_event() {
    g_clock++;
}
static void clock_recv(int64_t k) {
    g_clock = std::max(g_clock, k) + 1;
}

// ────────────────────────────────────────────────────────────────────────────
// Message handler (called from listener thread, *outside* g_mutex)
// ────────────────────────────────────────────────────────────────────────────
static void handle_message(const Message& m) {
    if (g_failed.load()) return; // failed process ignores messages

    switch (m.type) {

    case MsgType::REQ: {
        // RA81: decide immediately or defer
        bool send_now;
        {
            std::lock_guard<std::mutex> lk(g_mutex);
            clock_recv(m.timestamp);
            g_peer_req_ts[m.sender_id] = m.timestamp;

            // Priority: we defer if we want CS AND (our stamp < their stamp
            //           OR (equal stamps and our id < their id))
            bool we_have_priority =
                g_sc_wanted &&
                ( (g_osn < m.timestamp) ||
                  (g_osn == m.timestamp && MY_ID < m.sender_id) );

            if (we_have_priority) {
                g_deferred[m.sender_id] = true;
                send_now = false;
                log("Deferring REP to P" + std::to_string(m.sender_id) +
                    " (our osn=" + std::to_string(g_osn) +
                    " vs their ts=" + std::to_string(m.timestamp) + ")");
            } else {
                send_now = true;
            }
            write_state_json();
        }
        if (send_now) {
            log("Sending REP to P" + std::to_string(m.sender_id));
            send_rep(m.sender_id);
        }
        break;
    }

    case MsgType::REP: {
        bool can_enter = false;
        {
            std::lock_guard<std::mutex> lk(g_mutex);
            clock_recv(m.timestamp);
            if (g_sc_wanted && g_replies_needed > 0) {
                g_replies_needed--;
                log("Got REP from P" + std::to_string(m.sender_id) +
                    " -> replies_needed=" + std::to_string(g_replies_needed));
                if (g_replies_needed == 0)
                    can_enter = true;
            }
            write_state_json();
        }
        if (can_enter) g_cv.notify_one();
        break;
    }

    case MsgType::FAIL:
        log("P" + std::to_string(m.sender_id) + " reported FAIL");
        break;

    default:
        break;
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Listener thread: accepts TCP connections, reads one Message per connection
// ────────────────────────────────────────────────────────────────────────────
static void listener_thread() {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(process_port(MY_ID));
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(srv, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); exit(1);
    }
    listen(srv, 64);
    log("Listening on port " + std::to_string(process_port(MY_ID)));

    while (true) {
        int cli = accept(srv, nullptr, nullptr);
        if (cli < 0) continue;

        // Read exactly one Message
        Message wire;
        ssize_t n = recv(cli, &wire, sizeof(wire), MSG_WAITALL);
        close(cli);
        if (n != (ssize_t)sizeof(wire)) continue;

        // Deserialize
        Message m;
        m.type      = wire.type;
        m.sender_id = ntohl(wire.sender_id);
        m.timestamp = be64toh(wire.timestamp);
        memset(m.pad, 0, sizeof(m.pad));

        // Handle in a detached thread so listener stays responsive
        std::thread([m]{ handle_message(m); }).detach();
    }
}

// ────────────────────────────────────────────────────────────────────────────
// RA81 CS request / release
// ────────────────────────────────────────────────────────────────────────────

// [FIX 2] request_cs() retourne désormais un bool :
//   true  -> on a bien reçu toutes les REP et on est entré en CS
//   false -> timeout (un ou plusieurs peers n'ont pas répondu à temps) ou
//            panne déclenchée pendant l'attente ; aucune entrée en CS,
//            l'appelant doit sauter la section critique et retenter plus
//            tard (boucle principale).
static bool request_cs() {
    int64_t stamp;
    {
        std::lock_guard<std::mutex> lk(g_mutex);
        clock_event();
        g_osn            = g_clock;
        g_sc_wanted      = true;
        g_replies_needed = N - 1;
        g_state          = ProcState::WAITING;
        stamp            = g_osn;
        write_state_json();
    }
    log("Requesting CS with stamp=" + std::to_string(stamp));
    broadcast_req(stamp);

    // Attente bornée dans le temps des N-1 réponses (FIX 2)
    bool entered = false;
    std::vector<int> to_notify; // [FIX 3] REP différées à libérer si on abandonne

    {
        std::unique_lock<std::mutex> lk(g_mutex);
        bool ok = g_cv.wait_for(
            lk,
            std::chrono::milliseconds(REQUEST_TIMEOUT_MS),
            []{ return g_replies_needed == 0 || g_failed.load(); }
        );

        if (g_failed.load()) {
            // Panne déclenchée pendant l'attente : sig_handler a déjà
            // nettoyé g_sc_wanted / g_replies_needed (FIX 1), rien de plus
            // à faire ici.
            entered = false;
        } else if (ok && g_replies_needed == 0) {
            // Toutes les REP reçues à temps
            g_in_cs = true;
            g_state = ProcState::IN_CS;
            write_state_json();
            entered = true;
        } else {
            // Timeout expiré : un ou plusieurs peers n'ont pas répondu
            // (probablement FAILED). On abandonne cette tentative plutôt
            // que de bloquer indéfiniment.
            //
            // [FIX 3] BUG CRITIQUE CORRIGÉ ICI : pendant qu'on attendait,
            // on a pu différer des REP vers d'autres processus (g_deferred)
            // parce qu'on avait priorité sur eux (osn plus petit). Ces REP
            // ne sont normalement libérées que dans release_cs(). Mais si
            // on abandonne ici sans jamais entrer en CS, release_cs() ne
            // sera JAMAIS appelée pour cette tentative -> ces REP restent
            // différées pour toujours -> les processus qui les attendaient
            // timeoutent à leur tour en ayant eux-mêmes différé des REP ->
            // interblocage en cascade qui bloque tout le système.
            //
            // On doit donc, comme le ferait release_cs(), libérer
            // immédiatement toutes les REP qu'on devait avant d'abandonner.
            log("TIMEOUT waiting for REPs (still missing " +
                std::to_string(g_replies_needed) +
                ") -> abandoning this CS request, will retry next cycle");
            g_sc_wanted      = false;
            g_replies_needed = 0;
            g_state          = ProcState::IDLE;
            for (int j = 1; j <= N; j++) {
                if (g_deferred[j]) {
                    g_deferred[j] = false;
                    to_notify.push_back(j);
                }
            }
            write_state_json();
            entered = false;
        }
    }

    // [FIX 3] Envoi hors mutex des REP différées libérées par l'abandon
    if (!to_notify.empty()) {
        log("Flushing deferred REPs after timeout to: " + [&]{
            std::string s;
            for (int j : to_notify) s += std::to_string(j) + " ";
            return s;
        }());
        for (int j : to_notify) send_rep(j);
    }

    return entered;
}

static void release_cs() {
    std::vector<int> to_notify;
    {
        std::lock_guard<std::mutex> lk(g_mutex);
        g_in_cs     = false;
        g_sc_wanted = false;
        g_state     = ProcState::IDLE;
        clock_event();
        for (int j = 1; j <= N; j++) {
            if (g_deferred[j]) {
                g_deferred[j] = false;
                to_notify.push_back(j);
            }
        }
        write_state_json();
    }
    log("Released CS, sending deferred REPs to: " + [&]{
        std::string s;
        for (int j : to_notify) s += std::to_string(j) + " ";
        return s.empty() ? "(none)" : s;
    }());
    for (int j : to_notify) send_rep(j);
}

// ────────────────────────────────────────────────────────────────────────────
// Signal handler: toggle FAILED state (SIGUSR1)
// ────────────────────────────────────────────────────────────────────────────

// [FIX 1] Au moment de passer en FAILED, on collecte les REP différées
// qu'on devait à d'autres processus et on les envoie via un thread détaché
// (l'envoi réseau est bloquant et ne doit pas s'exécuter dans le contexte
// du signal handler lui-même). Sans ça, tout processus à qui on devait une
// REP restait bloqué pour toujours dans son g_cv.wait().
static void sig_handler(int) {
    bool was_failed = g_failed.exchange(!g_failed.load());
    std::vector<int> to_notify;

    {
        std::lock_guard<std::mutex> lk(g_mutex);
        if (!was_failed) {
            // Transition vers FAILED
            g_state = ProcState::FAILED;
            g_replies_needed = 0;
            g_sc_wanted = false;
            g_in_cs     = false;

            // [FIX 1] Collecte des REP différées à envoyer avant de couper
            for (int j = 1; j <= N; j++) {
                if (g_deferred[j]) {
                    g_deferred[j] = false;
                    to_notify.push_back(j);
                }
            }
        } else {
            // Récupération : retour à l'état IDLE normal
            g_state = ProcState::IDLE;
        }
        write_state_json();
    }

    if (!was_failed) {
        g_cv.notify_all();

        // [FIX 1] Envoi des REP différées en attente, dans un thread détaché
        if (!to_notify.empty()) {
            std::thread([to_notify]{
                for (int j : to_notify) {
                    send_rep(j);
                }
            }).detach();
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Main loop
// ────────────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 2) { std::cerr << "Usage: ra81_process <id>\n"; return 1; }
    MY_ID = std::atoi(argv[1]);
    if (MY_ID < 1 || MY_ID > N) { std::cerr << "id must be 1.." << N << "\n"; return 1; }

    signal(SIGUSR1, sig_handler);

    // Init state
    {
        std::lock_guard<std::mutex> lk(g_mutex);
        g_state = ProcState::IDLE;
        write_state_json();
    }

    // Start listener thread
    std::thread(listener_thread).detach();

    // Give listeners time to bind
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    log("Started. PID=" + std::to_string(getpid()));

    std::mt19937 rng(std::random_device{}() ^ (uint32_t)MY_ID * 2654435761u);
    std::uniform_int_distribution<int> idle_dist(1000, 5000); // ms
    std::uniform_int_distribution<int> cs_dist(1000, 2000);   // ms

    while (true) {
        // Check if failed
        if (g_failed.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        // Random idle period
        int idle_ms = idle_dist(rng);
        log("Idle for " + std::to_string(idle_ms) + "ms");
        std::this_thread::sleep_for(std::chrono::milliseconds(idle_ms));

        if (g_failed.load()) continue;

        // Request CS (FIX 2: peut désormais échouer par timeout)
        bool entered = request_cs();
        if (g_failed.load()) continue;
        if (!entered) continue; // timeout : on retente au prochain cycle

        // Critical section work
        int cs_ms = cs_dist(rng);
        log(">>> ENTERING CS for " + std::to_string(cs_ms) + "ms <<<");
        std::this_thread::sleep_for(std::chrono::milliseconds(cs_ms));
        log("<<< LEAVING CS >>>");

        // Release CS
        release_cs();
    }

    return 0;
}
