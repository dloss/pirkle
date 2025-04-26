use std::error::Error;
use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;
use prql_compiler as prqlc;
use rusqlite::{Connection, ToSql};

/// A command-line tool to query CSV and SQLite files using PRQL (PRQL Query Language)
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Files to query (CSV or SQLite), or 'stdin' to read from standard input
    #[arg(required = false)]
    files: Vec<PathBuf>,

    /// PRQL query string (after --)
    #[arg(last = true, num_args = 0..=1)]
    query_after_delimiter: Option<String>,

    /// PRQL query string (with --query flag)
    #[arg(long, value_name = "QUERY")]
    query: Option<String>,

    /// Show schema information for the provided files
    #[arg(long)]
    schema: bool,

    /// Output format (table, csv, json, logfmt)
    #[arg(short, long, default_value = "table", value_parser = ["table", "csv", "jsonl", "logfmt"])]
    format: String,

    /// Show generated SQL without executing
    #[arg(long)]
    show_sql: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    // Process file arguments to identify stdin markers
    let (regular_files, stdin_tables) = process_file_arguments(&cli.files)?;

    // Check for explicit schema request
    if cli.schema {
        return show_schemas(&regular_files, &stdin_tables);
    }

    // Determine the query source (prioritize --query over --)
    let query = cli.query.or(cli.query_after_delimiter).or_else(|| {
        // Only use stdin for query if not being used for data and it's not a terminal
        if stdin_tables.is_empty() && atty::isnt(atty::Stream::Stdin) {
            let mut buffer = String::new();
            if let Ok(_) = io::stdin().read_to_string(&mut buffer) {
                Some(buffer)
            } else {
                None
            }
        } else {
            None
        }
    });

    // If no query but files specified (including stdin markers), show schema
    if query.is_none() && (!regular_files.is_empty() || !stdin_tables.is_empty()) {
        return show_schemas(&regular_files, &stdin_tables);
    }

    // If no query and no files, show help
    if query.is_none() {
        eprintln!("Error: No query provided. Use --query, -- delimiter, or pipe a query.");
        eprintln!("Run with --help for usage information.");
        std::process::exit(1);
    }

    // Run the query with both regular files and stdin tables
    run_query(
        &query.unwrap(),
        &regular_files,
        &stdin_tables,
        &cli.format,
        cli.show_sql,
    )
}

// Function to process file arguments and identify stdin markers
fn process_file_arguments(
    files: &[PathBuf],
) -> Result<(Vec<PathBuf>, Vec<(String, String)>), Box<dyn Error>> {
    let mut regular_files = Vec::new();
    let mut stdin_tables = Vec::new();

    for file_arg in files {
        let file_str = file_arg.to_string_lossy();

        if file_str == "stdin" {
            // Plain "stdin" argument - use "stdin" as the table name
            stdin_tables.push(("stdin".to_string(), "stdin".to_string()));
        } else if let Some(custom_name) = file_str.strip_prefix("stdin:") {
            if !custom_name.is_empty() {
                // "stdin:custom" argument - use custom name as the table name
                stdin_tables.push((custom_name.to_string(), "stdin".to_string()));
            } else {
                return Err("Invalid stdin table specification: empty name after 'stdin:'".into());
            }
        } else {
            // Regular file - validate it exists
            if !file_arg.exists() {
                return Err(format!("File not found: {}", file_arg.display()).into());
            }
            regular_files.push(file_arg.clone());
        }
    }

    // If no stdin tables but also no files specified, and stdin is not a terminal,
    // implicitly add a stdin table
    if stdin_tables.is_empty() && regular_files.is_empty() && atty::isnt(atty::Stream::Stdin) {
        // Check if there's data in stdin before assuming it's for data input
        // We'll just peek at the first byte without consuming it
        let stdin = io::stdin();
        let mut handle = stdin.lock();

        // Try to peek if there's data
        let mut peek_buf = [0; 1];
        if handle.read_exact(&mut peek_buf).is_ok() {
            // There's data, so add default stdin table
            stdin_tables.push(("stdin".to_string(), "stdin".to_string()));
        }
        // Note: We've consumed a byte, but it'll be buffered and available for later reads
    }

    Ok((regular_files, stdin_tables))
}

// Function to convert Polars DataType to SQLite type string
fn polars_to_sqlite_type(dtype: &DataType) -> &'static str {
    match dtype {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "INTEGER",
        DataType::Float32 | DataType::Float64 => "REAL",
        DataType::Decimal(..) => "REAL",
        DataType::Date | DataType::Datetime(..) => "TEXT", // Could use INTEGER for Unix timestamp
        DataType::Time => "TEXT",
        DataType::Boolean => "INTEGER", // SQLite has no Boolean, use INTEGER (0/1)
        DataType::String => "TEXT",
        DataType::List(_) => "TEXT", // Store lists as JSON text
        DataType::Binary => "BLOB",
        _ => "TEXT", // Default to TEXT for any other types
    }
}

fn show_schemas(
    files: &[PathBuf],
    stdin_tables: &[(String, String)],
) -> Result<(), Box<dyn Error>> {
    // First show schemas for regular files
    for file in files {

        if file
            .extension()
            .map(|e| e == "sqlite" || e == "db")
            .unwrap_or(false)
        {
            // 1) Open the database
            let conn = Connection::open(file)?;

            // 2) List all user tables
            let mut tbl_stmt = conn.prepare(
                "SELECT name
                       FROM sqlite_master
                      WHERE type='table'
                        AND name NOT LIKE 'sqlite_%';",
            )?;
            let table_names = tbl_stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;

            // 3) For each table, inline PRAGMA table_info
            for table_name in table_names {
                println!("Table: {}", table_name);

                // PRAGMA cannot take parameters, so inline the table name
                let pragma_sql = format!("PRAGMA table_info('{}')", table_name);
                let mut col_stmt = conn.prepare(&pragma_sql)?;
                let columns = col_stmt.query_map([], |row| {
                    // row[1] = column name, row[2] = type
                    Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                })?;

                println!("Columns:");
                for col in columns {
                    let (name, typ) = col?;
                    println!("  {} ({})", name, typ);
                }
                println!();
            }
        } else {
            let table_name = file.file_stem().unwrap().to_string_lossy();
            println!("Table: {}", table_name); 

            // For CSV files, use Polars to get schema with types
            let df = CsvReader::from_path(file)?
                .infer_schema(Some(100))
                .has_header(true)
                .finish()?;

            println!("Columns:");
            for (name, dtype) in df.schema().iter() {
                let type_str = polars_to_sqlite_type(dtype);
                println!("  {} ({})", name, type_str);
            }
        }
        println!();
    }

    // Then show schemas for stdin tables if stdin has data
    if !stdin_tables.is_empty() && atty::isnt(atty::Stream::Stdin) {
        // Read stdin data into a buffer
        let mut buffer = Vec::new();
        io::stdin().read_to_end(&mut buffer)?;

        if !buffer.is_empty() {
            // Use Polars to infer schema from the buffer
            let df = CsvReader::new(io::Cursor::new(&buffer))
                .infer_schema(Some(100))
                .has_header(true)
                .finish()?;

            // Show schema for each stdin table (they all share the same structure)
            for (table_name, _) in stdin_tables {
                println!("Table: {}", table_name);
                println!("Columns:");
                for (name, dtype) in df.schema().iter() {
                    let type_str = polars_to_sqlite_type(dtype);
                    println!("  {} ({})", name, type_str);
                }
                println!();
            }
        } else {
            println!("Warning: Could not read data from stdin");
        }

        println!("Note: Full stdin data will be processed when query is executed.");
    } else if !stdin_tables.is_empty() {
        // Stdin tables were specified but no data is available
        println!("Warning: stdin tables specified, but no data available from stdin");
    }

    Ok(())
}

fn run_query(
    query: &str,
    files: &[PathBuf],
    stdin_tables: &[(String, String)],
    format: &str,
    show_sql: bool,
) -> Result<(), Box<dyn Error>> {
    let sql = compile_prql(query)?;

    if show_sql {
        println!("{}", sql);
        return Ok(());
    }

    let conn = Connection::open_in_memory()?;

    // Load regular files
    for file in files {
        let table_name = file.file_stem().unwrap().to_string_lossy();

        if file
            .extension()
            .map(|e| e == "sqlite" || e == "db")
            .unwrap_or(false)
        {
            conn.execute(
                &format!("ATTACH DATABASE '{}' AS '{}'", file.display(), table_name),
                [],
            )?;
        } else {
            load_csv_with_polars(&conn, &table_name.to_string(), file)?;
        }
    }

    // Load stdin data if needed
    if !stdin_tables.is_empty() && atty::isnt(atty::Stream::Stdin) {
        // Read stdin data once into memory
        let mut stdin_data = Vec::new();
        io::stdin().read_to_end(&mut stdin_data)?;

        if stdin_data.is_empty() {
            return Err("Stdin tables specified, but no data received from stdin".into());
        }

        // Create each requested table from the same stdin data
        for (table_name, _) in stdin_tables {
            load_csv_from_memory_with_polars(&conn, table_name, &stdin_data)?;
        }
    } else if !stdin_tables.is_empty() {
        return Err("Stdin tables specified, but no data available from stdin".into());
    }

    // Execute the query and format results
    let mut stmt = conn.prepare(&sql)?;

    // Run query and immediately collect rows into a Vec to free up stmt
    let collected_rows = stmt
        .query_map([], |row| {
            Ok((0..row.as_ref().column_count())
                .map(|i| match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => None,
                    rusqlite::types::ValueRef::Integer(i) => Some(i.to_string()),
                    rusqlite::types::ValueRef::Real(f) => Some(f.to_string()),
                    rusqlite::types::ValueRef::Text(t) => {
                        Some(String::from_utf8_lossy(t).to_string())
                    }
                    rusqlite::types::ValueRef::Blob(_) => Some("[BLOB]".to_string()),
                })
                .collect::<Vec<Option<String>>>())
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

    // Output
    match format {
        "csv" => {
            // Print headers first
            println!("{}", column_names.join(","));

            for row in &collected_rows {
                let flat = row
                    .iter()
                    .map(|v| v.clone().unwrap_or_else(|| "NULL".into()))
                    .collect::<Vec<_>>();
                println!("{}", flat.join(","));
            }
        }
        "jsonl" => {
            for row in &collected_rows {
                let json_obj: serde_json::Value = column_names
                    .iter()
                    .zip(row.iter())
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            match v {
                                Some(val_str) => {
                                    // Try to parse as number
                                    if let Ok(int_val) = val_str.parse::<i64>() {
                                        serde_json::Value::Number(int_val.into())
                                    } else if let Ok(float_val) = val_str.parse::<f64>() {
                                        // Create number from float (with some safeguards)
                                        match serde_json::Number::from_f64(float_val) {
                                            Some(num) => serde_json::Value::Number(num),
                                            None => serde_json::Value::String(val_str.clone()),
                                        }
                                    } else if val_str == "true" {
                                        serde_json::Value::Bool(true)
                                    } else if val_str == "false" {
                                        serde_json::Value::Bool(false)
                                    } else if val_str == "null" {
                                        serde_json::Value::Null
                                    } else {
                                        // Default to string for everything else
                                        serde_json::Value::String(val_str.clone())
                                    }
                                }
                                None => serde_json::Value::Null,
                            },
                        )
                    })
                    .collect::<serde_json::Map<_, _>>()
                    .into();
                println!("{}", serde_json::to_string(&json_obj)?);
            }
        }
        "logfmt" => {
            for row in &collected_rows {
                let mut line = String::new();
                for (k, v) in column_names.iter().zip(row.iter()) {
                    let val = v.clone().unwrap_or_else(|| "NULL".to_string());
                    line.push_str(&format!("{}=\"{}\" ", k, val.replace('"', "\\\"")));
                }
                println!("{}", line.trim_end());
            }
        }
        "table" | _ => {
            print_table(&column_names, &collected_rows);
        }
    }

    Ok(())
}

fn compile_prql(query: &str) -> Result<String, Box<dyn Error>> {
    if query.ends_with(".prql") && std::path::Path::new(query).exists() {
        let prql = fs::read_to_string(query)?;
        Ok(prqlc::compile(&prql, &prqlc::Options::default())?)
    } else {
        Ok(prqlc::compile(query, &prqlc::Options::default())?)
    }
}

fn print_table(headers: &[String], rows: &[Vec<Option<String>>]) {
    if rows.is_empty() {
        println!("No results.");
        return;
    }

    // Convert all values to strings and include headers
    let mut table: Vec<Vec<String>> = vec![];
    table.push(headers.to_vec()); // first row: headers
    for row in rows {
        table.push(
            row.iter()
                .map(|v| v.clone().unwrap_or_else(|| "NULL".into()))
                .collect(),
        );
    }

    // Compute max width per column
    let col_widths = (0..headers.len())
        .map(|col| table.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();

    // Print the table
    for (i, row) in table.iter().enumerate() {
        for (j, cell) in row.iter().enumerate() {
            print!("{:width$}  ", cell, width = col_widths[j]);
        }
        println!();
        if i == 0 {
            // separator after header
            for width in &col_widths {
                print!("{:-<width$}--", "", width = *width);
            }
            println!();
        }
    }
}

// Helper function to safely convert Polars AnyValue to SQLite value
fn convert_any_value_to_sql(value: AnyValue) -> Box<dyn ToSql> {
    match value {
        AnyValue::Null => Box::new(Option::<String>::None),
        AnyValue::Int8(v) => Box::new(v as i64),
        AnyValue::Int16(v) => Box::new(v as i64),
        AnyValue::Int32(v) => Box::new(v as i64),
        AnyValue::Int64(v) => Box::new(v),
        AnyValue::UInt8(v) => Box::new(v as i64),
        AnyValue::UInt16(v) => Box::new(v as i64),
        AnyValue::UInt32(v) => Box::new(v as i64),
        AnyValue::UInt64(v) => Box::new(v as i64),
        AnyValue::Float32(v) => Box::new(v as f64),
        AnyValue::Float64(v) => Box::new(v),
        AnyValue::Boolean(v) => Box::new(if v { 1i64 } else { 0i64 }),
        AnyValue::String(v) => Box::new(v.to_string()),
        // Convert other types to strings
        _ => Box::new(value.to_string()),
    }
}

// New function to load CSV using Polars with type inference
fn load_csv_with_polars(
    conn: &Connection,
    table_name: &str,
    path: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    // Use Polars to read the CSV with type inference
    let df = CsvReader::from_path(path)?
        .infer_schema(Some(100))
        .has_header(true)
        .finish()?;

    // Create table with appropriate column types
    let mut create_table_sql = format!("CREATE TABLE '{}' (", table_name);
    let columns = df
        .schema()
        .iter()
        .map(|(name, dtype)| {
            let sqlite_type = polars_to_sqlite_type(dtype);
            format!("'{}' {}", name, sqlite_type)
        })
        .collect::<Vec<_>>()
        .join(", ");

    create_table_sql.push_str(&columns);
    create_table_sql.push_str(")");

    conn.execute(&create_table_sql, [])?;

    // Prepare placeholders for the insert statement
    let placeholders = vec!["?"; df.width()].join(", ");
    let insert_sql = format!("INSERT INTO '{}' VALUES ({})", table_name, placeholders);

    // Insert data row by row without using a prepared statement
    for row_idx in 0..df.height() {
        let mut params: Vec<Box<dyn ToSql>> = Vec::with_capacity(df.width());

        for col_idx in 0..df.width() {
            let series = &df.get_columns()[col_idx];
            let value = series.get(row_idx);
            match value {
                Ok(any_value) => params.push(convert_any_value_to_sql(any_value)),
                Err(_) => params.push(Box::new(Option::<String>::None)),
            }
        }

        let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();

        conn.execute(&insert_sql, &param_refs[..])?;
    }

    Ok(())
}

// Updated function to load CSV from memory using Polars
fn load_csv_from_memory_with_polars(
    conn: &Connection,
    table_name: &str,
    data: &[u8],
) -> Result<(), Box<dyn Error>> {
    // Use Polars to read the CSV with type inference
    let df = CsvReader::new(io::Cursor::new(data))
        .infer_schema(Some(100))
        .has_header(true)
        .finish()?;

    // Create table with appropriate column types
    let mut create_table_sql = format!("CREATE TABLE '{}' (", table_name);
    let columns = df
        .schema()
        .iter()
        .map(|(name, dtype)| {
            let sqlite_type = polars_to_sqlite_type(dtype);
            format!("'{}' {}", name, sqlite_type)
        })
        .collect::<Vec<_>>()
        .join(", ");

    create_table_sql.push_str(&columns);
    create_table_sql.push_str(")");

    conn.execute(&create_table_sql, [])?;

    // Prepare placeholders for the insert statement
    let placeholders = vec!["?"; df.width()].join(", ");
    let insert_sql = format!("INSERT INTO '{}' VALUES ({})", table_name, placeholders);

    // Insert data row by row
    for row_idx in 0..df.height() {
        let mut params: Vec<Box<dyn ToSql>> = Vec::with_capacity(df.width());

        for col_idx in 0..df.width() {
            let series = &df.get_columns()[col_idx];
            let value = series.get(row_idx);
            match value {
                Ok(any_value) => params.push(convert_any_value_to_sql(any_value)),
                Err(_) => params.push(Box::new(Option::<String>::None)),
            }
        }

        let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();

        conn.execute(&insert_sql, &param_refs[..])?;
    }

    Ok(())
}
