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
# Ensure Pirkle is built before running tests if using target/debug/pirkle
# cargo build # Potentially add this if not using 'cargo run --'
PIRKLE_BIN="cargo run --release --" # Use release for speed, ensure it's built
# PIRKLE_BIN="./target/release/pirkle" # Or point to pre-built binary

# Create test directory
TESTDIR="test_data"
mkdir -p $TESTDIR

# Generated SQLite files for cleanup
OUTPUT_EMPLOYEES_SQLITE="$TESTDIR/output_employees.sqlite"
OUTPUT_FILTERED_EMPLOYEES_SQLITE="$TESTDIR/output_filtered_employees.sqlite"
OUTPUT_COMPANY_SQLITE="$TESTDIR/output_company.sqlite"
OUTPUT_STDIN_CUSTOMERS_SQLITE="$TESTDIR/output_stdin_customers.sqlite"

GENERATED_SQLITE_FILES=(
    "$OUTPUT_EMPLOYEES_SQLITE"
    "$OUTPUT_FILTERED_EMPLOYEES_SQLITE"
    "$OUTPUT_COMPANY_SQLITE"
    "$OUTPUT_STDIN_CUSTOMERS_SQLITE"
)

# Cleanup function
cleanup() {
    echo "Cleaning up generated files..."
    for file in "${GENERATED_SQLITE_FILES[@]}"; do
        rm -f "$file"
    done
    # rm -rf $TESTDIR # Optionally remove the whole test_data dir
}

# Trap EXIT signal to ensure cleanup
trap cleanup EXIT

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
    "{\"age\":32,.*\"name\":\"John Smith\".*}"

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

# --- New tests for --output-db ---

echo
echo -e "${YELLOW}--- Running tests for --output-db feature ---${NC}"

# Helper function for SQLite assertions
assert_sqlite_output() {
    local test_name="$1"
    local db_file="$2"
    local query="$3"
    local expected="$4"
    local description="$5"

    echo "SQLite Assertion for $test_name ($description): Querying '$query' in $db_file"
    local actual
    if ! actual=$(sqlite3 "$db_file" "$query" 2>&1); then
        echo -e "${RED}FAILED: SQLite query failed for $test_name ($description).${NC}"
        echo "Query: $query"
        echo "Error: $actual"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi

    if [[ "$actual" == "$expected" ]]; then
        echo -e "${GREEN}PASSED: SQLite assertion for $test_name ($description).${NC}"
        # TESTS_PASSED is incremented by the calling test function
    else
        echo -e "${RED}FAILED: SQLite assertion for $test_name ($description).${NC}"
        echo "Query: $query"
        echo "Expected: '$expected'"
        echo "Actual:   '$actual'"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
    return 0
}

# Test Case 1: CSV to SQLite
TEST_NAME_1="Output DB: CSV to SQLite"
echo -e "${YELLOW}Running test: ${TEST_NAME_1}${NC}"
if $PIRKLE_BIN examples/employees.csv --output-db "$OUTPUT_EMPLOYEES_SQLITE"; then
    if [ ! -f "$OUTPUT_EMPLOYEES_SQLITE" ]; then
        echo -e "${RED}FAILED: $OUTPUT_EMPLOYEES_SQLITE was not created.${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    else
        echo "File $OUTPUT_EMPLOYEES_SQLITE created."
        TEST_1_SUCCESS=true
        assert_sqlite_output "$TEST_NAME_1" "$OUTPUT_EMPLOYEES_SQLITE" ".table employees" "employees" "Table 'employees' exists" || TEST_1_SUCCESS=false
        # Schema check: just check column names for simplicity, types can be tricky with SQLite's dynamic typing
        # Expected schema for employees.csv: id,name,department,age,salary,country
        # .schema employees output is like: CREATE TABLE employees(...)
        # We'll check for column names within the CREATE TABLE statement
        SCHEMA_OUTPUT=$(sqlite3 "$OUTPUT_EMPLOYEES_SQLITE" ".schema employees")
        if ! echo "$SCHEMA_OUTPUT" | grep -q "id" || \
           ! echo "$SCHEMA_OUTPUT" | grep -q "name" || \
           ! echo "$SCHEMA_OUTPUT" | grep -q "department" || \
           ! echo "$SCHEMA_OUTPUT" | grep -q "age" || \
           ! echo "$SCHEMA_OUTPUT" | grep -q "salary" || \
           ! echo "$SCHEMA_OUTPUT" | grep -q "country"; then
            echo -e "${RED}FAILED: Schema check for $TEST_NAME_1. Not all columns found.${NC}"
            echo "Schema output: $SCHEMA_OUTPUT"
            TEST_1_SUCCESS=false
        else
            echo -e "${GREEN}PASSED: Schema check for $TEST_NAME_1.${NC}"
        fi
        
        EXPECTED_EMP_ROWS=$(($(wc -l < examples/employees.csv) - 1))
        assert_sqlite_output "$TEST_NAME_1" "$OUTPUT_EMPLOYEES_SQLITE" "SELECT COUNT(*) FROM employees;" "$EXPECTED_EMP_ROWS" "Row count for employees" || TEST_1_SUCCESS=false
        # Sample row: John Smith,Engineering,32,85000,USA (id=1)
        assert_sqlite_output "$TEST_NAME_1" "$OUTPUT_EMPLOYEES_SQLITE" "SELECT name, department, age, salary, country FROM employees WHERE id = 1;" "John Smith|Engineering|32|85000.0|USA" "Sample row data" || TEST_1_SUCCESS=false
        
        if $TEST_1_SUCCESS; then
            echo -e "${GREEN}PASSED: ${TEST_NAME_1}${NC}"
            TESTS_PASSED=$((TESTS_PASSED + 1))
        else
            echo -e "${RED}FAILED: Some checks failed for ${TEST_NAME_1}${NC}"
            # TESTS_FAILED is already incremented by assert_sqlite_output
        fi
    fi
else
    echo -e "${RED}FAILED: Pirkle command failed for $TEST_NAME_1.${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi


# Test Case 2: CSV with Query to SQLite
TEST_NAME_2="Output DB: CSV with Query to SQLite"
echo -e "${YELLOW}Running test: ${TEST_NAME_2}${NC}"
QUERY_FOR_TEST_2="from employees | filter country == 'USA' | select {name, age}"
if $PIRKLE_BIN examples/employees.csv --query "$QUERY_FOR_TEST_2" --output-db "$OUTPUT_FILTERED_EMPLOYEES_SQLITE"; then
    if [ ! -f "$OUTPUT_FILTERED_EMPLOYEES_SQLITE" ]; then
        echo -e "${RED}FAILED: $OUTPUT_FILTERED_EMPLOYEES_SQLITE was not created.${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    else
        echo "File $OUTPUT_FILTERED_EMPLOYEES_SQLITE created."
        TEST_2_SUCCESS=true
        # Verify the original 'employees' table is saved, not the query result as a new table.
        assert_sqlite_output "$TEST_NAME_2" "$OUTPUT_FILTERED_EMPLOYEES_SQLITE" ".table employees" "employees" "Table 'employees' exists" || TEST_2_SUCCESS=false
        
        EXPECTED_EMP_ROWS_CASE2=$(($(wc -l < examples/employees.csv) - 1)) # Full table is saved
        assert_sqlite_output "$TEST_NAME_2" "$OUTPUT_FILTERED_EMPLOYEES_SQLITE" "SELECT COUNT(*) FROM employees;" "$EXPECTED_EMP_ROWS_CASE2" "Row count for employees" || TEST_2_SUCCESS=false
        assert_sqlite_output "$TEST_NAME_2" "$OUTPUT_FILTERED_EMPLOYEES_SQLITE" "SELECT name FROM employees WHERE id = 1;" "John Smith" "Sample data from employees table" || TEST_2_SUCCESS=false

        if $TEST_2_SUCCESS; then
            echo -e "${GREEN}PASSED: ${TEST_NAME_2}${NC}"
            TESTS_PASSED=$((TESTS_PASSED + 1))
        else
            echo -e "${RED}FAILED: Some checks failed for ${TEST_NAME_2}${NC}"
        fi
    fi
else
    echo -e "${RED}FAILED: Pirkle command failed for $TEST_NAME_2.${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi


# Test Case 3: Multiple CSVs to SQLite
TEST_NAME_3="Output DB: Multiple CSVs to SQLite"
echo -e "${YELLOW}Running test: ${TEST_NAME_3}${NC}"
if $PIRKLE_BIN examples/employees.csv examples/departments.csv --output-db "$OUTPUT_COMPANY_SQLITE"; then
    if [ ! -f "$OUTPUT_COMPANY_SQLITE" ]; then
        echo -e "${RED}FAILED: $OUTPUT_COMPANY_SQLITE was not created.${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    else
        echo "File $OUTPUT_COMPANY_SQLITE created."
        TEST_3_SUCCESS=true
        assert_sqlite_output "$TEST_NAME_3" "$OUTPUT_COMPANY_SQLITE" ".table employees" "employees" "Table 'employees' exists" || TEST_3_SUCCESS=false
        assert_sqlite_output "$TEST_NAME_3" "$OUTPUT_COMPANY_SQLITE" ".table departments" "departments" "Table 'departments' exists" || TEST_3_SUCCESS=false
        
        EXPECTED_EMP_ROWS_CASE3=$(($(wc -l < examples/employees.csv) - 1))
        assert_sqlite_output "$TEST_NAME_3" "$OUTPUT_COMPANY_SQLITE" "SELECT COUNT(*) FROM employees;" "$EXPECTED_EMP_ROWS_CASE3" "Row count for employees" || TEST_3_SUCCESS=false
        
        EXPECTED_DEPT_ROWS_CASE3=$(($(wc -l < examples/departments.csv) - 1))
        assert_sqlite_output "$TEST_NAME_3" "$OUTPUT_COMPANY_SQLITE" "SELECT COUNT(*) FROM departments;" "$EXPECTED_DEPT_ROWS_CASE3" "Row count for departments" || TEST_3_SUCCESS=false

        if $TEST_3_SUCCESS; then
            echo -e "${GREEN}PASSED: ${TEST_NAME_3}${NC}"
            TESTS_PASSED=$((TESTS_PASSED + 1))
        else
            echo -e "${RED}FAILED: Some checks failed for ${TEST_NAME_3}${NC}"
        fi
    fi
else
    echo -e "${RED}FAILED: Pirkle command failed for $TEST_NAME_3.${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi


# Test Case 4: Stdin CSV to SQLite
TEST_NAME_4="Output DB: Stdin CSV to SQLite"
echo -e "${YELLOW}Running test: ${TEST_NAME_4}${NC}"
if cat examples/customers.csv | $PIRKLE_BIN stdin --output-db "$OUTPUT_STDIN_CUSTOMERS_SQLITE"; then
    if [ ! -f "$OUTPUT_STDIN_CUSTOMERS_SQLITE" ]; then
        echo -e "${RED}FAILED: $OUTPUT_STDIN_CUSTOMERS_SQLITE was not created.${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    else
        echo "File $OUTPUT_STDIN_CUSTOMERS_SQLITE created."
        TEST_4_SUCCESS=true
        assert_sqlite_output "$TEST_NAME_4" "$OUTPUT_STDIN_CUSTOMERS_SQLITE" ".table stdin" "stdin" "Table 'stdin' exists" || TEST_4_SUCCESS=false
        
        # Schema check for customers.csv: customer_id,name,email,city
        SCHEMA_STDIN_OUTPUT=$(sqlite3 "$OUTPUT_STDIN_CUSTOMERS_SQLITE" ".schema stdin")
        if ! echo "$SCHEMA_STDIN_OUTPUT" | grep -q "customer_id" || \
           ! echo "$SCHEMA_STDIN_OUTPUT" | grep -q "name" || \
           ! echo "$SCHEMA_STDIN_OUTPUT" | grep -q "email" || \
           ! echo "$SCHEMA_STDIN_OUTPUT" | grep -q "city"; then
            echo -e "${RED}FAILED: Schema check for $TEST_NAME_4. Not all columns found in 'stdin' table.${NC}"
            echo "Schema output: $SCHEMA_STDIN_OUTPUT"
            TEST_4_SUCCESS=false
        else
            echo -e "${GREEN}PASSED: Schema check for $TEST_NAME_4.${NC}"
        fi

        EXPECTED_CUST_ROWS=$(($(wc -l < examples/customers.csv) - 1))
        assert_sqlite_output "$TEST_NAME_4" "$OUTPUT_STDIN_CUSTOMERS_SQLITE" "SELECT COUNT(*) FROM stdin;" "$EXPECTED_CUST_ROWS" "Row count for stdin (customers)" || TEST_4_SUCCESS=false

        if $TEST_4_SUCCESS; then
            echo -e "${GREEN}PASSED: ${TEST_NAME_4}${NC}"
            TESTS_PASSED=$((TESTS_PASSED + 1))
        else
            echo -e "${RED}FAILED: Some checks failed for ${TEST_NAME_4}${NC}"
        fi
    fi
else
    echo -e "${RED}FAILED: Pirkle command failed for $TEST_NAME_4.${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi


# Re-print summary at the end
echo
echo "--- Final Test Summary ---"
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