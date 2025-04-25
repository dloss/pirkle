#!/bin/bash
set -e

echo "Generating company.sqlite..."

rm -f company.sqlite

sqlite3 company.sqlite <<EOF
CREATE TABLE employees (
  id INTEGER,
  name TEXT,
  age INTEGER,
  country TEXT,
  department_id INTEGER,
  salary INTEGER,
  position TEXT
);

CREATE TABLE departments (
  department_id INTEGER,
  department_name TEXT
);

.mode csv
.import employees.csv employees
.import departments.csv departments
EOF

echo "Generating customers.sqlite..."

rm -f customers.sqlite

sqlite3 customers.sqlite <<EOF
CREATE TABLE customers (
  customer_id INTEGER,
  name TEXT,
  region TEXT
);

.mode csv
.import customers.csv customers
EOF

echo "Done."
