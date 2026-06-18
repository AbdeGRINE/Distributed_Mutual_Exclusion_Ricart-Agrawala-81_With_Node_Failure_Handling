#pragma once
#include <cstdint>
#include <string>
#include <array>

// ── Configuration ──────────────────────────────────────────────────────────
static constexpr int N = 10;                 // number of processes (1..N)
static constexpr int BASE_PORT = 9000;       // process i listens on 9000+i
static constexpr const char* HOST = "127.0.0.1";

// Port encoding: process i ↦ BASE_PORT + i  (i ∈ [1..N])
inline int process_port(int id) { return BASE_PORT + id; }

// ── Message types ──────────────────────────────────────────────────────────
enum class MsgType : uint8_t {
    REQ = 1,   // request to enter CS
    REP = 2,   // reply / permission
    FAIL = 3,  // process announces failure (optional broadcast)
    ALIVE= 4,  // heartbeat / alive notification
};

// Wire format: fixed-size 12-byte message
struct Message {
    MsgType  type;       // 1 byte
    uint8_t  pad[3];     // alignment
    int32_t  sender_id;  // sender process id (1..N)
    int64_t  timestamp;  // logical clock value (Lamport)
};
static_assert(sizeof(Message) == 16, "Message size mismatch");

// ── Process states (for GUI) ───────────────────────────────────────────────
enum class ProcState : uint8_t {
    IDLE   = 0,   // not requesting CS
    WAITING= 1,   // waiting for N-1 replies
    IN_CS  = 2,   // inside critical section
    FAILED = 3,   // simulated crash
};

inline const char* state_name(ProcState s) {
    switch(s) {
        case ProcState::IDLE:    return "IDLE";
        case ProcState::WAITING: return "WAITING";
        case ProcState::IN_CS:   return "IN_CS";
        case ProcState::FAILED:  return "FAILED";
    }
    return "?";
}

// ── Shared state file (JSON) written by each process ─────────────────────
// Each process writes /tmp/ra81_proc_<id>.json every time its state changes.
// The GUI dashboard polls these files.
inline std::string state_file(int id) {
    return std::string("/tmp/ra81_proc_") + std::to_string(id) + ".json";
}
