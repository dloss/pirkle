# PRQL CSV CLI

A command-line tool to query CSV files using [PRQL](https://prql-lang.org/).

## Installation

First, make sure you have Rust and Cargo installed. If not, install them from [rustup.rs](https://rustup.rs/).

Then, you can install this tool by cloning this repository and building it:

```bash
git clone https://github.com/yourusername/prql-csv.git
cd prql-csv
cargo install --path .
```

## Usage

### Run a query against CSV files

```bash
# Basic query with table output (default)
prql-csv query "from employees | filter country == 'USA' | select {name, age}" ./employees.csv

# Multiple CSV files
prql-csv query "from employees | join departments (==department_id) | select {name, department_name}" ./employees.csv ./departments.csv

# Output as CSV
prql-csv query --format csv "from employees | filter salary > 50000" ./employees.csv
```

### View the SQL generated from a PRQL query

```bash
prql-csv show-sql "from employees | select {name, age} | sort age"
```

### Use a query from a file

Create a file `my_query.prql` with your PRQL code, then:

```bash
prql-csv query my_query.prql ./data.csv
```

## Examples

**Example 1:** Find the average age by department

```bash
prql-csv query "from employees | group department (aggregate {avg_age = average age})" ./employees.csv
```

**Example 2:** Get the top 5 highest paid employees

```bash
prql-csv query "from employees | sort -salary | take 5 | select {name, position, salary}" ./employees.csv
```

**Example 3:** Calculate stats from multiple tables

```bash
prql-csv query "from orders | join customers (==customer_id) | group region (aggregate {total_revenue = sum amount, order_count = count this})" ./orders.csv ./customers.csv
```

## License

MIT