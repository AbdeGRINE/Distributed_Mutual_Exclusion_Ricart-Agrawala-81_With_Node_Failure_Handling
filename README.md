# RA81 — Distributed Mutual Exclusion Simulation (Ricart & Agrawala 1981)

**SYSR – 2CS · ESI · By D.E. MENACER**

## Project Structure

```
ra81_sim/
├── include/
│   └── ra81_common.h       # Shared types, constants, ports
├── src/
│   └── ra81_process.cpp    # Full RA81 implementation
├── gui/
│   └── dashboard.html      # Real-time graphical interface
├── ra81_server.py          # HTTP server for the dashboard
└── Makefile
```

## Build

```bash
make
```
Requires: `g++ ≥ 7`, C++17, POSIX (Linux/macOS).

## Full Startup

### Option 1 — Makefile (everything in one command)
```bash
make start          # Launches the 10 processes in the background
python3 ra81_server.py 8080  # Launches the dashboard server
# Open http://localhost:8080
```

### Option 2 — Manual
```bash
# Terminal 1..10 (or in the background):
./ra81_process 1 &
./ra81_process 2 &
...
./ra81_process 10 &

# Dashboard terminal:
python3 ra81_server.py 8080
```

### Stop
```bash
make stop
```

## TCP Port Encoding

Each process `i` (1..10) listens on port **9000 + i**:
- P1 → 9001, P2 → 9002, ..., P10 → 9010
- Defined in `include/ra81_common.h`: `process_port(id) = BASE_PORT + id`

## RA81 Algorithm Implementation

### Local variables per process Pᵢ
- `g_clock`: Lamport clock
- `g_osn`: timestamp of the current request
- `g_replies_needed`: number of REP messages still expected (initialized to N-1)
- `g_sc_wanted`: Pᵢ wants to enter the critical section
- `g_deferred[j]`: deferred reply toward Pⱼ

### Protocol (3 steps)

**1. Requesting entry into the critical section:**
```
clock++ ; osn = clock ; sc_wanted = true ; replies_needed = N-1
Broadcast (REQ, osn, i) to all others
Wait until (replies_needed == 0)
```

**2. Receiving a REQ from Pⱼ:**
```
If sc_wanted AND (osn < ts_j OR (osn == ts_j AND i < j)):
    Defer the reply  →  deferred[j] = true
Else:
    Send (REP) to j immediately
```

**3. Exiting the critical section:**
```
sc_wanted = false
For every j where deferred[j]: send (REP) to j
```

### Complexity
- **2(N-1) messages** per entry into the critical section: (N-1) REQ + (N-1) REP

### Dijkstra's Conditions Satisfied
- ✅ **Mutual exclusion**: only one process in the critical section at a time
- ✅ **Bounded waiting**: total order on timestamps prevents starvation
- ✅ **Deadlock-free**: no possible deadlock
- ✅ **Fairness**: priority based on timestamp + ID (tie-breaker)

## Failure Simulation

### Injecting a Failure (SIGUSR1)
```bash
make fail ID=3      # Toggle FAILURE on P3
# or directly:
kill -SIGUSR1 <PID_of_P3>
```

From the dashboard: click **"⚡ Simulate Failure"** on the process card.

### Behavior on Failure
A failed process (`FAILED`):
- No longer listens for incoming REQ messages
- No longer sends REQ messages
- Cancels its pending REP wait, if any
- Other processes become blocked if the failed process
  was supposed to send them a REP (a limitation of base RA81,
  without full fault tolerance — see the extension with
  ABSENT/RETURNED messages).

Sending SIGUSR1 again → **restart** (IDLE state).

## Graphical Interface — Dashboard

Available at `http://localhost:8080`

![Dashboard screenshot](Screenshot%20From%202026-06-18%2016-03-39.png)
| Zone | Content |
|------|---------|
| **Process cards** | State, Lamport clock, osn timestamp, REP messages awaited (progress bar), deferred replies, failure button |
| **REQ matrix** | For each pair (receiver, sender): the latest received REQ timestamp |
| **Log** | Timestamps of state transitions (IDLE→WAITING→IN_CS→IDLE) and deferrals |
| **Stats bar** | Global counters: in critical section, waiting, failures, max clock, total CS accesses |

Configurable refresh rate: 400ms / 800ms / 1.5s / 3s.

## Text Logs

```bash
tail -f /tmp/ra81_1.log    # P1 log
make watch ID=5            # P5 log
```

## References

- Ricart G., Agrawala A.K. (1981). *An optimal algorithm for mutual exclusion in computer networks.* CACM 24(1):9-17.
- Lamport L. (1978). *Time, clocks and the ordering of events in a distributed system.* CACM 21(7):558-565.
- D.E. MENACER, *SYSR – 2CS Chapter 5: Distributed Mutual Exclusion*, ESI 2026.
