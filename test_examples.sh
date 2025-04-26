#!/bin/bash
set -e

# Color definitions
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# Path to pirkle binary - adjust as needed
PIRKLE_BIN="cargo run --"
# PIRKLE_BIN="./target/release/pirkle" # Uncomment for release testing

# Create test directory
TESTDIR="test_data"
mkdir -p $TESTDIR

# Function to run a test
run_test() {
    local name=$1
    local cmd=$2
    local expected_pattern=$3

    echo -e "${YELLOW}Running test: ${name}${NC}"
    echo "Command: $cmd"
    
    # Run the command and capture output
    local output
    if ! output=$(eval "$cmd" 2>&1); then
        echo -e "${RED}FAILED: Command exited with non-zero status${NC}"
        echo "Output: $output"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
    
    # Replace newlines with a special character for pattern matching
    local flat_output=$(echo "$output" | tr '\n' '§')
    
    # Check output against expected pattern (using the flattened output)
    if echo "$flat_output" | grep -q "$expected_pattern"; then
        echo -e "${GREEN}PASSED${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}FAILED: Output doesn't match expected pattern${NC}"
        echo "Expected to find: $expected_pattern"
        echo "Actual output: $output"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
    
    return 0
}

# Function to handle tests that might be skipped
maybe_run_test() {
    local name=$1
    local cmd=$2
    local expected_pattern=$3
    local requirement=$4
    
    if [ -n "$requirement" ] && ! eval "$requirement"; then
        echo -e "${YELLOW}SKIPPED: ${name} (requirement not met: $requirement)${NC}"
        TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
        return 0
    fi
    
    run_test "$name" "$cmd" "$expected_pattern"
}

# Prepare test data
echo "Preparing test data..."

# Make sure example files exist
if [ ! -d "examples" ]; then
    echo -e "${RED}Error: examples directory not found${NC}"
    exit 1
fi

# Create a test query file
cat > $TESTDIR/test_query.prql << EOF
from employees
filter country == "USA"
select {name, age}
EOF

# Run tests for each example in the README

# Basic query with --query flag
run_test "Basic query with --query" \
    "$PIRKLE_BIN examples/employees.csv --query \"from employees | filter country == 'USA' | select {name, age}\"" \
    "John Smith.*32"

# Query a SQLite file (skip if not available)
maybe_run_test "Query SQLite file" \
    "$PIRKLE_BIN examples/company.sqlite --query \"from employees | select {name, age}\"" \
    "name.*age" \
    "[ -f examples/company.sqlite ]"

# Alternative syntax with -- delimiter
run_test "Query with -- delimiter" \
    "$PIRKLE_BIN examples/employees.csv -- \"from employees | filter department == 'Engineering' | select {name, age}\"" \
    "John Smith.*32"

# Using a here-doc for multi-line query
cat > $TESTDIR/here_doc_test.sh << 'EOF'
#!/bin/bash
PIRKLE_BIN="$1"
$PIRKLE_BIN examples/employees.csv << QUERY
from employees
filter department == "Engineering"
sort age
select {name, age}
QUERY
EOF
chmod +x $TESTDIR/here_doc_test.sh
run_test "Query with here-doc" \
    "$TESTDIR/here_doc_test.sh \"$PIRKLE_BIN\"" \
    "Ahmed Hassan.*29"

# Reading from a file with pipe
run_test "Query from piped file" \
    "cat $TESTDIR/test_query.prql | $PIRKLE_BIN examples/employees.csv" \
    "John Smith.*32"

# Schema display
run_test "Schema display" \
    "$PIRKLE_BIN examples/employees.csv --schema" \
    "Table: employees§Columns:§  id"

# Show SQL without executing
run_test "Show SQL without executing" \
    "$PIRKLE_BIN examples/employees.csv --query \"from employees | filter country == 'USA'\" --show-sql" \
    "SELECT.*FROM.*employees.*WHERE.*country = 'USA'"

# Show SQL for a PRQL file
run_test "Show SQL for PRQL file" \
    "$PIRKLE_BIN examples/employees.csv --query examples/queries/avg_age_by_department.prql --show-sql" \
    "SELECT.*department_id.*AVG.*age"

# Output in CSV format
run_test "Output in CSV format" \
    "$PIRKLE_BIN examples/employees.csv --format csv --query \"from employees | filter salary > 70000\"" \
    "1,John Smith,Engineering,32,85000,USA"

# Output in JSON Lines format
run_test "Output in JSON Lines format" \
    "$PIRKLE_BIN examples/employees.csv --format jsonl --query \"from employees | filter country == 'USA'\"" \
    "{\"age\":\"32\",.*\"name\":\"John Smith\".*}"

# Output in logfmt format
run_test "Output in logfmt format" \
    "$PIRKLE_BIN examples/employees.csv --format logfmt --query \"from employees | filter country == 'USA'\"" \
    "id=\"1\" name=\"John Smith\""

# Using a PRQL file
# First create a fixed version of the PRQL file if it doesn't exist
mkdir -p $TESTDIR/queries
cat > $TESTDIR/queries/top_5_paid.prql << EOF
from employees
sort -salary
select {name, department, salary}
take 5
EOF

# FIXME:
#run_test "Using a PRQL file" \
#    "$PIRKLE_BIN examples/employees.csv --query $TESTDIR/queries/top_5_paid.prql" \
#    "Robert Johnson.*Engineering.*92000"

# Joining tables
mkdir -p $TESTDIR/queries
cat > $TESTDIR/queries/join_query.prql << EOF
from orders
join customers (==customer_id)
select {orders.order_id, customers.name, orders.amount}
EOF

run_test "Joining tables" \
    "$PIRKLE_BIN examples/orders.csv examples/customers.csv --query $TESTDIR/queries/join_query.prql" \
    "Acme Corp.*250"

# Print summary
echo 
echo -e "${GREEN}Tests passed: $TESTS_PASSED${NC}"
echo -e "${RED}Tests failed: $TESTS_FAILED${NC}"
echo -e "${YELLOW}Tests skipped: $TESTS_SKIPPED${NC}"

# Exit with error if any tests failed
if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All executed tests passed!${NC}"
    exit 0
fi