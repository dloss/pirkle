[![Releases](https://img.shields.io/github/v/release/dloss/pirkle)](https://github.com/dloss/pirkle/releases)
[![crates.io](https://img.shields.io/crates/v/pirkle.svg)](https://crates.io/crates/pirkle)
[![CI](https://github.com/dloss/pirkle/actions/workflows/release.yml/badge.svg)](https://github.com/dloss/pirkle/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

# Pirkle — Query CSV and SQLite with PRQL

Pirkle is a fast, lightweight command-line tool that brings the power of [PRQL](https://prql-lang.org/) (Pipelined Relational Query Language) to CSV and SQLite files. Transform, filter, and join your data with expressive, readable queries that compile to optimized SQL.

**Why Pirkle?**
- 🚀 **Fast**: Built in Rust with optimized SQLite backend
- 📊 **Flexible**: Query CSV files as if they were database tables
- 🔗 **Powerful**: Join multiple files and data sources
- 📝 **Readable**: PRQL's pipeline syntax is intuitive and maintainable
- 🎯 **Versatile**: Multiple output formats (table, CSV, JSON, logfmt)

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
  - [Basic Queries](#basic-queries)
  - [Reading from Standard Input](#reading-from-standard-input)
  - [Output Formats](#output-formats)
  - [Advanced Features](#advanced-features)
- [Common Use Cases](#common-use-cases)
- [Performance Tips](#performance-tips)
- [Troubleshooting](#troubleshooting)
- [Requirements](#requirements)
- [Example Data](#example-data)
- [License](#license)

## Features

- Query CSV files as structured tables
- Join CSV and SQLite files together
- Write expressive queries using PRQL
- Output as a pretty table, CSV, JSON, or logfmt
- Inspect the generated SQL
- View schema information for files
- Lightweight, fast, and written in Rust

## Installation

### Prebuilt Binaries

Download the latest release for your platform:

| Platform | Download |
|----------|----------|
| Windows | [pirkle-x86_64-pc-windows-msvc.zip](https://github.com/dloss/pirkle/releases/latest) |
| macOS (Apple Silicon) | [pirkle-aarch64-apple-darwin.tar.gz](https://github.com/dloss/pirkle/releases/latest) |
| macOS (Intel) | [pirkle-x86_64-apple-darwin.tar.gz](https://github.com/dloss/pirkle/releases/latest) |
| Linux (x86_64) | [pirkle-x86_64-unknown-linux-musl.tar.gz](https://github.com/dloss/pirkle/releases/latest) |

### Package Managers

```bash
# Cargo (Rust)
cargo install pirkle

# Homebrew (coming soon)
# brew install pirkle
```

### From Source

Install using [Rust](https://rustup.rs/):

```bash
git clone https://github.com/dloss/pirkle.git
cd pirkle
cargo install --path .
```

## Quick Start

```bash
# Install pirkle
cargo install pirkle

# Query a CSV file
pirkle examples/data.csv --query "from data | filter price > 100 | select {name, price}"

# View file structure
pirkle examples/data.csv --schema
```

## Usage

### Basic Queries

```bash
# Query a CSV file. CSV files are auto-loaded as SQLite tables.
$ pirkle examples/employees.csv --query "from employees | filter country == 'USA' | select {name, age}"
name            age  
---------------------
John Smith      32   
Robert Johnson  41   
James Brown     39   
```

```bash
# Query a SQLite file
$ pirkle examples/company.sqlite --query "from employees | select {name, age} | take 5"
name            age  
---------------------
John Smith      32   
Maria Garcia    28   
Robert Johnson  41   
Lisa Wang       35   
Ahmed Hassan    29   
```

```bash
# Alternative syntax using -- delimiter
$ pirkle examples/employees.csv -- "from employees | filter department == 'Engineering' | select {name, age}"
name            age  
---------------------
John Smith      32   
Robert Johnson  41   
Ahmed Hassan    29   
Sarah Kim       31   
```

### Reading from Standard Input

Pirkle supports reading CSV data from standard input, making it easy to pipe data from other commands:

```bash
# Pipe data into pirkle
$ cat examples/employees.csv | pirkle stdin --query "from stdin | filter salary > 70000"
id  name             department   age  salary  country      
------------------------------------------------------------
1   John Smith       Engineering  32   85000   USA          
3   Robert Johnson   Engineering  41   92000   USA          
5   Ahmed Hassan     Engineering  29   75000   Egypt        
8   Sarah Kim        Engineering  31   83000   South Korea  
9   James Brown      Sales        39   85000   USA          
10  Fatima Al-Farsi  Marketing    36   76000   UAE

# Use stdin with files
$ cat examples/orders.csv | pirkle stdin examples/customers.csv --query "from stdin | join customers (==customer_id)"
order_id  customer_id  amount  region  customer_id  name              region  
------------------------------------------------------------------------------
1         100          250     North   100          Acme Corp         North   
2         101          300     South   101          Globex Inc        South   
3         100          150     North   100          Acme Corp         North   
4         102          400     West    102          Initech           West    
5         103          200     East    103          Stark Industries  East

# Custom table name for stdin data
$ cat examples/employees.csv | pirkle stdin:workers --query "from workers | sort {-salary}"
id  name              department   age  salary  country      
-------------------------------------------------------------
3   Robert Johnson    Engineering  41   92000   USA          
1   John Smith        Engineering  32   85000   USA          
9   James Brown       Sales        39   85000   USA          
8   Sarah Kim         Engineering  31   83000   South Korea  
10  Fatima Al-Farsi   Marketing    36   76000   UAE          
5   Ahmed Hassan      Engineering  29   75000   Egypt        
4   Lisa Wang         Marketing    35   70000   China        
7   Carlos Rodriguez  Marketing    33   68000   Spain        
2   Maria Garcia      Sales        28   65000   Mexico       
6   Emma Wilson       Sales        27   62000   UK
```

#### Pipeline Integration

Pirkle integrates seamlessly with Unix pipelines:

```bash
# From curl/API responses
curl -s api.example.com/data.csv | pirkle stdin --query "from stdin | filter active == true"

# From other command output
cat *.csv | pirkle stdin --query "from stdin | group category (aggregate {count = count this})"

# Complex pipeline
grep "ERROR" logs.csv | pirkle stdin --query "
from stdin 
| derive hour = (timestamp | date.truncate hour)
| group hour (aggregate {error_count = count this})
| sort hour"
```

##### Key features:

- **Auto-detection**: Data on stdin is loaded as a table named "stdin"
- **Explicit reference**: Use the filename `stdin` to read from stdin
- **Custom naming**: Use `stdin:tablename` for custom table names
- **Query from stdin**: If no query is provided with `--query` or `--`, Pirkle will read the query from stdin:
  ```bash
  $ echo "from employees | filter country == 'USA'" | pirkle examples/employees.csv
  ```
- **Multiple references**: Use the same stdin data with different table names
  ```bash
  $ cat examples/employees.csv | pirkle stdin:workers stdin:staff --query "from workers | join staff (==id)"
  ```

Pirkle intelligently determines how to use stdin based on your command arguments, making it a flexible tool for data pipelines.

### Viewing Schema Information

To see the structure of your tables:

```bash
# View schemas with the --schema flag
$ pirkle examples/employees.csv --schema
Table: employees
Columns:
  id (INTEGER)
  name (TEXT)
  department (TEXT)
  age (INTEGER)
  salary (INTEGER)
  country (TEXT)
```

### Show SQL without executing

You can use the `--show-sql` flag to see the SQL that would be generated without executing the query:

```bash
$ pirkle examples/employees.csv --query "from employees | filter country == 'USA'" --show-sql
SELECT
  *
FROM
  employees
WHERE
  country = 'USA'
-- Generated by PRQL compiler version:0.12.2 (https://prql-lang.org)
```

This also works with PRQL files:

```bash
$ pirkle examples/employees.csv --query examples/queries/avg_age_by_department.prql --show-sql
SELECT
  department_id,
  AVG(age) AS avg_age
FROM
  employees
GROUP BY
  department_id
-- Generated by PRQL compiler version:0.12.2 (https://prql-lang.org)
```

### Output Formats

| Format | Use Case | Example |
|--------|----------|---------|
| `table` | Human-readable terminal output | Data exploration |
| `csv` | Spreadsheet import, further processing | `pirkle data.csv --format csv > result.csv` |
| `jsonl` | API integration, log analysis | `pirkle logs.csv --format jsonl \| jq '.'` |
| `logfmt` | Structured logging, monitoring | Integration with log aggregators |

Default is a readable table format.

To output CSV:

```bash
$ pirkle examples/employees.csv --format csv --query "from employees | filter salary > 70000"
1,John Smith,Engineering,32,85000,USA
3,Robert Johnson,Engineering,41,92000,USA
5,Ahmed Hassan,Engineering,29,75000,Egypt
8,Sarah Kim,Engineering,31,83000,South Korea
9,James Brown,Sales,39,85000,USA
10,Fatima Al-Farsi,Marketing,36,76000,UAE
```

Other supported formats:

```bash
# JSON Lines format
$ pirkle examples/employees.csv --format jsonl --query "from employees | filter country == 'USA'"
{"age":32,"country":"USA","department":"Engineering","id":1,"name":"John Smith","salary":85000}
{"age":41,"country":"USA","department":"Engineering","id":3,"name":"Robert Johnson","salary":92000}
{"age":39,"country":"USA","department":"Sales","id":9,"name":"James Brown","salary":85000}
```

```bash
# logfmt format
$ pirkle examples/employees.csv --format logfmt --query "from employees | filter country == 'USA'"
id="1" name="John Smith" department="Engineering" age="32" salary="85000" country="USA"
id="3" name="Robert Johnson" department="Engineering" age="41" salary="92000" country="USA"
id="9" name="James Brown" department="Sales" age="39" salary="85000" country="USA"
```

### Using PRQL files

You can use prewritten PRQL query files:

```bash
# Use a PRQL file directly with --query
$ pirkle examples/employees.csv --query examples/queries/top_5_paid.prql
name                  department    salary
---------------------------------------
Robert Johnson        Engineering   92000
John Smith            Engineering   85000
James Brown           Sales         85000
Sarah Kim             Engineering   83000
Fatima Al-Farsi       Marketing     76000
```

### Joining tables

To join tables, use the join operation:

```bash
$ pirkle examples/orders.csv examples/customers.csv --query "from orders
join customers (==customer_id)
select {orders.order_id, customers.name, orders.amount}"
order_id   name            amount
-------------------------------
1          Acme Corp       250
2          Globex Inc      300
3          Acme Corp       150
4          Initech         400
5          Stark Industries 200
```

## Common Use Cases

### Data Analysis
```bash
# Find average salary and employee count by department
pirkle employees.csv --query "
from employees 
| group department (aggregate {
    avg_salary = average salary, 
    count = count this
  })
| sort -avg_salary
| take 5"
```

### Data Cleaning
```bash
# Remove duplicates and filter valid records
pirkle messy_data.csv --query "
from messy_data
| filter email != null
| group email (take 1)
| select {name, email, phone}"
```

### Joining Data Sources
```bash
# Combine sales data with customer information
pirkle sales.csv customers.csv --query "
from sales
| join customers (==customer_id)
| group customers.region (aggregate {total_sales = sum sales.amount})
| sort -total_sales"
```

### Time Series Analysis
```bash
# Analyze daily sales trends
pirkle transactions.csv --query "
from transactions
| derive date = (timestamp | date.truncate day)
| group date (aggregate {
    daily_sales = sum amount,
    transaction_count = count this
  })
| sort date"
```

### Data Exploration
```bash
# Quick summary statistics
pirkle dataset.csv --query "
from dataset
| aggregate {
    min_value = min price,
    max_value = max price,
    avg_value = average price,
    total_records = count this
  }"
```

## Performance Tips

- **Schema inference**: Pirkle automatically detects column types for optimal performance
- **Memory usage**: Large CSV files are streamed efficiently through SQLite
- **Query optimization**: PRQL compiles to optimized SQL - complex queries often perform better than you'd expect
- **File formats**: SQLite files are queried directly without loading into memory
- **Early filtering**: For large datasets, filter early in your pipeline to reduce processing overhead

## Troubleshooting

### Common Issues

**File not found errors**
```bash
# Ensure file paths are correct
pirkle ./data/employees.csv --schema
```

**Query syntax errors**
```bash
# Use --show-sql to debug generated SQL
pirkle data.csv --query "your query here" --show-sql
```

**Large file performance**
```bash
# For very large files, consider filtering early in the pipeline
pirkle large_file.csv --query "from large_file | filter date > @2024-01-01 | ..."
```

**Memory issues with large datasets**
```bash
# Process data in chunks or use more specific filters
pirkle huge_file.csv --query "from huge_file | filter region == 'US' | take 1000"
```

## Example Data

Included example files:

- `examples/employees.csv`: Employee data with department, salary, and country information
- `examples/departments.csv`: Department names and IDs
- `examples/customers.csv`, `examples/orders.csv`: Customer-order relationship data
- `examples/queries/*.prql`: Sample PRQL queries

## License

MIT