CXX      = g++
CXXFLAGS = -std=c++17 -O2 -pthread -Wall -Wextra -I./include
TARGET   = ra81_process

all: $(TARGET)

$(TARGET): src/ra81_process.cpp include/ra81_common.h
	$(CXX) $(CXXFLAGS) -o $@ src/ra81_process.cpp

clean:
	rm -f $(TARGET)

# ── Convenience targets ────────────────────────────────────────────────────
# Start all 10 processes in background (logs to /tmp/ra81_<id>.log)
start: $(TARGET)
	@echo "Starting 10 RA81 processes..."
	@for i in $$(seq 1 10); do \
		./$(TARGET) $$i > /tmp/ra81_$$i.log 2>&1 & \
		echo "  Started P$$i (PID=$$!)"; \
	done
	@echo "All processes started. Logs: /tmp/ra81_<id>.log"
	@echo "State files: /tmp/ra81_proc_<id>.json"

# Stop all processes
stop:
	@pkill -f "ra81_process" 2>/dev/null || true
	@echo "All ra81_process instances stopped."

# Simulate failure of process ID (make fail ID=3)
fail:
	@kill -SIGUSR1 $$(pgrep -f "ra81_process $(ID)") && echo "Toggled FAIL on P$(ID)"

# Watch logs for a specific process (make watch ID=1)
watch:
	@tail -f /tmp/ra81_$(ID).log

.PHONY: all clean start stop fail watch
