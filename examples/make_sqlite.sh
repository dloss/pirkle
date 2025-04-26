#!/bin/bash
set -e

echo "Generating company.sqlite..."

rm -f company.sqlite

# Create temporary files without headers
for file in employees.csv departments.csv; do
  tail -n +2 "$file" > "tmp_$file"
done

sqlite3 company.sqlite <<EOF
CREATE TABLE employees (
  id INTEGER,
  name TEXT,
  department TEXT,
  age INTEGER,
  salary INTEGER,
  country TEXT
);

CREATE TABLE departments (
  department_id INTEGER,
  department_name TEXT
);

.mode csv
.import tmp_employees.csv employees
.import tmp_departments.csv departments
EOF

echo "Generating customers.sqlite..."

rm -f customers.sqlite

# Create temporary file without header
tail -n +2 customers.csv > tmp_customers.csv

sqlite3 customers.sqlite <<EOF
CREATE TABLE customers (
  customer_id INTEGER,
  name TEXT,
  region TEXT
);

.mode csv
.import tmp_customers.csv customers
EOF

echo "Generating orders.sqlite..."

rm -f orders.sqlite

# Create temporary file without header
tail -n +2 orders.csv > tmp_orders.csv

sqlite3 orders.sqlite <<EOF
CREATE TABLE orders (
  order_id INTEGER,
  customer_id INTEGER,
  amount INTEGER,
  region TEXT
);

.mode csv
.import tmp_orders.csv orders
EOF

# Clean up temporary files
rm -f tmp_*.csv

echo "Done."