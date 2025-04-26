#!/bin/bash
set -e

# Color definitions
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Path to pirkle binary - adjust as needed
PIRKLE_BIN="cargo run --"

echo -e "${YELLOW}Testing Polars CSV inference...${NC}"

# Test schema display with type inference
echo "Testing schema display with type inference..."
OUTPUT=$($PIRKLE_BIN examples/employees.csv --schema)

# Check if the schema output contains proper type information
if echo "$OUTPUT" | grep -q "id (INTEGER)"; then
    echo -e "${GREEN}✓ Schema correctly shows 'id' as INTEGER type${NC}"
else
    echo -e "${RED}✗ Failed to detect 'id' as INTEGER type${NC}"
    echo "$OUTPUT"
    exit 1
fi

if echo "$OUTPUT" | grep -q "age (INTEGER)"; then
    echo -e "${GREEN}✓ Schema correctly shows 'age' as INTEGER type${NC}"
else
    echo -e "${RED}✗ Failed to detect 'age' as INTEGER type${NC}"
    echo "$OUTPUT"
    exit 1
fi

if echo "$OUTPUT" | grep -q "name (TEXT)"; then
    echo -e "${GREEN}✓ Schema correctly shows 'name' as TEXT type${NC}"
else
    echo -e "${RED}✗ Failed to detect 'name' as TEXT type${NC}"
    echo "$OUTPUT"
    exit 1
fi

# Test a simple query with inferred types
echo "Testing query with inferred numeric types..."
OUTPUT=$($PIRKLE_BIN examples/employees.csv --query "from employees | filter age > 30 | select {name, age, salary}")

# Verify data is returned correctly
if echo "$OUTPUT" | grep -q "John Smith"; then
    echo -e "${GREEN}✓ Query returns expected results${NC}"
else
    echo -e "${RED}✗ Query failed to return expected results${NC}"
    echo "$OUTPUT"
    exit 1
fi

# Test a numeric aggregation
echo "Testing numeric aggregation with inferred types..."
OUTPUT=$($PIRKLE_BIN examples/employees.csv --query "from employees | aggregate {avg_age = average age, total_salary = sum salary}")

# Verify aggregation worked correctly (with numeric types)
if echo "$OUTPUT" | grep -q "avg_age"; then
    echo -e "${GREEN}✓ Aggregation query returned results${NC}"
else
    echo -e "${RED}✗ Aggregation query failed${NC}"
    echo "$OUTPUT"
    exit 1
fi

# Test joining tables with inferred types
echo "Testing join with inferred types..."
OUTPUT=$($PIRKLE_BIN examples/orders.csv examples/customers.csv --query "from orders | join customers (==customer_id) | select {orders.order_id, customers.name, orders.amount}")

# Verify join worked correctly
if echo "$OUTPUT" | grep -q "Acme Corp"; then
    echo -e "${GREEN}✓ Join query returned expected results${NC}"
else
    echo -e "${RED}✗ Join query failed${NC}"
    echo "$OUTPUT"
    exit 1
fi

echo -e "${GREEN}All Polars integration tests passed!${NC}"
exit 0