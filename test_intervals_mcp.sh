#!/bin/bash
# Test all Intervals.icu MCP tools via mcporter
# Usage: ./test-intervals-mcp.sh [activity_id]
# Output: ~/intervals-mcp-test.log

LOG=~/intervals-mcp-test.log
ACTIVITY_ID="${1:-i127016178}"
DATE="2026-02-23"

echo "========================================" | tee "$LOG"
echo "Intervals.icu MCP Tool Test" | tee -a "$LOG"
echo "$(date)" | tee -a "$LOG"
echo "Activity ID: $ACTIVITY_ID" | tee -a "$LOG"
echo "========================================" | tee -a "$LOG"

PASS=0
FAIL=0
SKIP=0

run_test() {
    local name="$1"
    shift
    local cmd="mcporter call $*"
    
    echo "" >> "$LOG"
    echo "--- $name ---" >> "$LOG"
    echo "CMD: $cmd" >> "$LOG"
    
    output=$(eval "$cmd" 2>&1)
    exit_code=$?
    
    echo "$output" >> "$LOG"
    
    if [ $exit_code -ne 0 ] || echo "$output" | grep -qi "error\|offline\|failed"; then
        echo "❌ $name" | tee -a "$LOG"
        FAIL=$((FAIL + 1))
    else
        # Check for empty/useless responses
        if echo "$output" | grep -qE '^\{\s*\}$|"value":\s*\{\s*\}|"value":\s*\[\s*\]'; then
            echo "⚠️  $name (empty response)" | tee -a "$LOG"
            FAIL=$((FAIL + 1))
        else
            echo "✅ $name" | tee -a "$LOG"
            PASS=$((PASS + 1))
        fi
    fi
}

skip_test() {
    local name="$1"
    local reason="$2"
    echo "⏭️  $name — SKIPPED ($reason)" | tee -a "$LOG"
    SKIP=$((SKIP + 1))
}

echo ""
echo "=== 1. Activity Discovery & Details ===" | tee -a "$LOG"
run_test "get_recent_activities" "intervals.get_recent_activities limit=3"
run_test "get_activities_around" "intervals.get_activities_around activity_id=$ACTIVITY_ID limit=3"
run_test "get_activities_csv" "intervals.get_activities_csv days_back=7 limit=3"
run_test "get_activity_details" "intervals.get_activity_details activity_id=$ACTIVITY_ID"

echo ""
echo "=== 2. Activity Deep Analysis ===" | tee -a "$LOG"
run_test "get_activity_streams" "intervals.get_activity_streams activity_id=$ACTIVITY_ID"
run_test "get_activity_intervals" "intervals.get_activity_intervals activity_id=$ACTIVITY_ID"
run_test "get_best_efforts (watts/300s)" "intervals.get_best_efforts activity_id=$ACTIVITY_ID stream=watts duration=300"
run_test "get_best_efforts (heartrate/300s)" "intervals.get_best_efforts activity_id=$ACTIVITY_ID stream=heartrate duration=300"

echo ""
echo "=== 3. Performance Curves ===" | tee -a "$LOG"
run_test "get_power_curves (42d)" "intervals.get_power_curves type=Ride days_back=42"
run_test "get_hr_curves (42d)" "intervals.get_hr_curves type=Ride days_back=42"
run_test "get_pace_curves (42d)" "intervals.get_pace_curves type=Run days_back=42"

echo ""
echo "=== 4. Histograms ===" | tee -a "$LOG"
run_test "get_power_histogram" "intervals.get_power_histogram activity_id=$ACTIVITY_ID"
run_test "get_hr_histogram" "intervals.get_hr_histogram activity_id=$ACTIVITY_ID"
run_test "get_pace_histogram" "intervals.get_pace_histogram activity_id=$ACTIVITY_ID"
run_test "get_gap_histogram" "intervals.get_gap_histogram activity_id=$ACTIVITY_ID"

echo ""
echo "=== 5. Wellness & Recovery ===" | tee -a "$LOG"
run_test "get_wellness (7d)" "intervals.get_wellness days_back=7"
run_test "get_wellness_for_date" "intervals.get_wellness_for_date date=$DATE"
run_test "get_fitness_summary" "intervals.get_fitness_summary"

echo ""
echo "=== 6. Athlete & Settings ===" | tee -a "$LOG"
run_test "get_athlete_profile" "intervals.get_athlete_profile"
run_test "get_sport_settings" "intervals.get_sport_settings"
run_test "get_gear_list" "intervals.get_gear_list"

echo ""
echo "=== 7. Workouts & Planning ===" | tee -a "$LOG"
run_test "get_upcoming_workouts" "intervals.get_upcoming_workouts days_ahead=7"
run_test "get_workout_library" "intervals.get_workout_library"

echo ""
echo "=== 8. Events & Calendar ===" | tee -a "$LOG"
run_test "get_events" "intervals.get_events days_back=90"

echo ""
echo "=== 9. Write/Update Tools (SKIPPED) ===" | tee -a "$LOG"
skip_test "create_activity" "write operation"
skip_test "update_activity" "write operation"
skip_test "delete_activity" "write operation"
skip_test "upload_activity" "write operation"
skip_test "update_wellness" "write operation"
skip_test "create_workout" "write operation"
skip_test "update_workout" "write operation"
skip_test "delete_workout" "write operation"
skip_test "schedule_workout" "write operation"
skip_test "move_workout" "write operation"
skip_test "create_workout_folder" "write operation"
skip_test "update_workout_folder" "write operation"
skip_test "delete_workout_folder" "write operation"
skip_test "create_event" "write operation"
skip_test "update_event" "write operation"
skip_test "delete_event" "write operation"
skip_test "update_sport_settings" "write operation"
skip_test "update_athlete_profile" "write operation"
skip_test "pair_activities" "write operation"
skip_test "unpair_activities" "write operation"

echo ""
echo "========================================" | tee -a "$LOG"
echo "RESULTS: ✅ $PASS passed | ❌ $FAIL failed | ⏭️  $SKIP skipped" | tee -a "$LOG"
echo "Log: $LOG" | tee -a "$LOG"
echo "========================================" | tee -a "$LOG"
