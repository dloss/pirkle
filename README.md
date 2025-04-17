# Pirkle — Query CSV and SQLite with PRQL

Pirkle is a command-line tool to query CSV and SQLite files using the [PRQL](https://prql-lang.org/) language.

It loads CSV files into an in-memory SQLite database — allowing you to join them with other tables, apply filters, and export results in table or CSV format.

---

## Features

- Query CSV files as structured tables
- Join CSV and SQLite files together
- Write expressive queries using PRQL
- Output as a pretty table or CSV
- Inspect the generated SQL
- Lightweight, fast, and written in Rust

> Note: While Pirkle does not depend on system libraries, it uses several Rust crates like `clap`, `prql-compiler`, `rusqlite`, and `serde`.

---

## Installation

Requires [Rust](https://rustup.rs/).

```bash
git clone https://github.com/yourusername/pirkle.git
cd pirkle
cargo install --path .
```

---

## Usage

### Query CSV and SQLite files

```bash
# Query a CSV file
pirkle query "from employees | filter country == 'USA' | select {name, age}" ./employees.csv

# Query a SQLite file
pirkle query "from employees | select {name, age}" ./company.sqlite

# Join CSV and SQLite
pirkle query "from employees | join departments (==department_id) | select {employees.name, departments.department_name}" ./employees.csv ./company.sqlite
```

CSV files are automatically loaded into in-memory SQLite tables, named after their filename (without the `.csv` extension).

---

### Output formats

Default is a readable table format.

To output CSV:

```bash
pirkle query --format csv "from employees | filter salary > 50000" ./employees.csv
```

---

### Other options

- Show generated SQL:
  ```bash
  pirkle show-sql "from employees | select {name, age} | sort age"
  ```

- Load query from a `.prql` file:
  ```bash
  pirkle query --from query.prql ./employees.csv
  ```

---

## Example Data

Included example files:

- `employees.csv`, `departments.csv`: Basic employee/department data
- `company.sqlite`: Preloaded version of the same
- `customers.csv`, `orders.csv`: Customer-order scenario

### Sample Query

```bash
pirkle query "
  from employees
  | join departments (==department_id)
  | filter salary > 60000
  | sort salary
  | select {employees.name, departments.department_name, salary}
" ./employees.csv ./departments.csv
```

---

## License

MIT
