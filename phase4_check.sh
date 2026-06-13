#!/bin/bash
# phase4_check.sh

set -e
echo "=== [1/6] Build ==="
go build ./... && echo "PASS" || echo "FAIL"

echo ""
echo "=== [2/6] Unit Tests ==="
go test -run "TestTimingWheel|TestAutoRenew" -count=1 2>&1 | grep -E "^(ok|---)" 

echo ""
echo "=== [3/6] Escape Analysis ==="
esc=$(go build -gcflags="-m" ./... 2>&1 | grep "turbolock/" | grep "escapes to heap" | grep -v "_test.go" | grep -v "context.Context" | grep -v "func literal" | wc -l)
echo "turbolock unexpected escapes: $esc (expect 0)"

echo ""
echo "=== [4/6] Benchmark ==="
go test -bench=BenchmarkTurboLock_HighConcurrency -benchmem -count=1 2>&1 | grep "BenchmarkTurboLock"

echo ""
echo "=== [5/6] Race Detection ==="
go test -race -run "TestTimingWheel_Concurrent" -count=1 2>&1 | grep -E "PASS|FAIL|DATA RACE"

echo ""
echo "=== [6/6] MaxHoldDuration Test ==="
go test -race -run "TestAutoRenew_NoGoroutineLeak" -count=1 2>&1 | grep -E "PASS|FAIL"

echo ""
echo "=== DONE ==="