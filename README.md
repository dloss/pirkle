# Pirkle — Query CSV and SQLite with PRQL

A tiny command-line tool to query CSV and SQLite files using [PRQL](https://prql-lang.org/).

Pirkle loads CSV files into an in-memory SQLite database — so you can query them like regular tables, join them with SQLite databases, and output the results in table or CSV format.

## Installation

First, install Rust from [rustup.rs](https://rustup.rs/).

Then:

```bash
git clone https://github.com/yourusername/pirkle.git
cd pirkle
cargo install --path .
```

## Usage

### Query CSV and SQLite files with PRQL

```bash
# Query a CSV file
pirkle query "from employees | filter country == 'USA' | select {name, age}" ./employees.csv

# Query a SQLite database file
pirkle query "from employees | select {name, age}" ./company.sqlite

# Join across multiple CSV and SQLite files
pirkle query "from employees | join departments (==department_id) | select {employees.name, departments.department_name}" ./employees.csv ./company.sqlite
```

> Note: CSV files are automatically loaded into in-memory SQLite tables based on their filename.

---

### Output formats

Default output is a simple table.

Use `--format csv` for CSV output:

```bash
pirkle query --format csv "from employees | filter salary > 50000" ./employees.csv
```

---

### Show the generated SQL (without running it)

```bash
pirkle show-sql "from employees | select {name, age} | sort age"
```

---

### Load a query from a `.prql` file

Write your query into `query.prql`:

```prql
from employees
filter salary > 50000
select {name, position, salary}
```

Then run:

```bash
pirkle query query.prql ./employees.csv
```

---

## Examples

### Average age by department (CSV)

```bash
pirkle query "from employees | group department (aggregate {avg_age = average age})" ./employees.csv
```

---

### Top 5 highest paid employees (SQLite)

```bash
pirkle query "from employees | sort -salary | take 5 | select {name, position, salary}" ./company.sqlite
```

---

### Revenue stats from multiple files (CSV + SQLite)

```bash
pirkle query "from orders | join customers (==customer_id) | group region (aggregate {total_revenue = sum amount, order_count = count this})" ./orders.csv ./customers.sqlite
```

---

## Features

- Query CSV and SQLite files with PRQL
- Auto-load CSV files into in-memory SQLite
- Join across multiple files (CSV or SQLite)
- Output as table or CSV
- Show generated SQL for debugging
- Lightweight — no dependencies beyond SQLite and PRQL compiler

---

## License

MIT