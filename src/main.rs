use std::error::Error;
use std::fs;
use std::io::{self, BufRead, Read};
use std::path::PathBuf;

use clap::Parser;
use csv::Reader;
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

fn show_schemas(
    files: &[PathBuf],
    stdin_tables: &[(String, String)],
) -> Result<(), Box<dyn Error>> {
    // First show schemas for regular files
    for file in files {
        let table_name = file.file_stem().unwrap().to_string_lossy();
        println!("Table: {}", table_name);

        if file
            .extension()
            .map(|e| e == "sqlite" || e == "db")
            .unwrap_or(false)
        {
            // For SQLite files, query schema information
            let conn = Connection::open(file)?;
            let mut stmt = conn.prepare("SELECT name, type FROM pragma_table_info(?)")?;
            let columns = stmt.query_map([table_name.as_ref()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;

            println!("Columns:");
            for column in columns {
                let (name, type_) = column?;
                println!("  {} ({})", name, type_);
            }
        } else {
            // For CSV files, read headers
            let mut reader = Reader::from_path(file)?;
            let headers = reader.headers()?;

            println!("Columns:");
            for header in headers {
                println!("  {} (TEXT)", header);
            }
        }
        println!();
    }

    // Then show schemas for stdin tables if stdin has data
    if !stdin_tables.is_empty() && atty::isnt(atty::Stream::Stdin) {
        // Use BufReader to read just a few lines to determine headers
        let stdin = io::stdin();
        let mut buf_reader = io::BufReader::new(stdin.lock());

        let mut header_buffer = String::new();
        if buf_reader.read_line(&mut header_buffer).is_ok() && !header_buffer.is_empty() {
            // Read one more line to have enough data to infer types
            let mut data_buffer = String::new();
            let _ = buf_reader.read_line(&mut data_buffer);

            // Combine header and sample data
            let sample = header_buffer + &data_buffer;

            // Parse the header
            let mut reader = Reader::from_reader(sample.as_bytes());
            if let Ok(headers) = reader.headers() {
                // Show schema for each stdin table (they all share the same structure)
                for (table_name, _) in stdin_tables {
                    println!("Table: {}", table_name);
                    println!("Columns:");
                    for header in headers {
                        println!("  {} (TEXT)", header);
                    }
                    println!();
                }
            } else {
                println!("Warning: Could not parse headers from stdin");
            }
        } else {
            println!("Warning: Could not read headers from stdin");
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
            load_csv(&conn, &table_name.to_string(), file)?;
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
            load_csv_from_memory(&conn, table_name, &stdin_data)?;
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
        // Replace the JSONL format section in run_query function with this:
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

fn load_csv(conn: &Connection, table_name: &str, path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let mut reader = Reader::from_path(path)?;
    let headers = reader.headers()?.clone();

    let columns = headers
        .iter()
        .map(|h| format!("'{}' TEXT", h))
        .collect::<Vec<_>>()
        .join(", ");

    conn.execute(&format!("CREATE TABLE '{}' ({})", table_name, columns), [])?;

    let mut stmt = conn.prepare(&format!(
        "INSERT INTO '{}' VALUES ({})",
        table_name,
        vec!["?"; headers.len()].join(", ")
    ))?;

    for result in reader.records() {
        let record = result?;
        let params = record
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>();

        let params_refs = params.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>();

        stmt.execute(&params_refs[..])?;
    }

    Ok(())
}

// Helper function to load CSV data directly from memory
fn load_csv_from_memory(
    conn: &Connection,
    table_name: &str,
    data: &[u8],
) -> Result<(), Box<dyn Error>> {
    let mut reader = Reader::from_reader(data);
    let headers = reader.headers()?.clone();

    let columns = headers
        .iter()
        .map(|h| format!("'{}' TEXT", h))
        .collect::<Vec<_>>()
        .join(", ");

    conn.execute(&format!("CREATE TABLE '{}' ({})", table_name, columns), [])?;

    let mut stmt = conn.prepare(&format!(
        "INSERT INTO '{}' VALUES ({})",
        table_name,
        vec!["?"; headers.len()].join(", ")
    ))?;

    for result in reader.records() {
        let record = result?;
        let params = record
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>();

        let params_refs = params.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>();

        stmt.execute(&params_refs[..])?;
    }

    Ok(())
}
