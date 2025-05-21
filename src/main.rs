use std::error::Error;
use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;
use prql_compiler as prqlc;
use rusqlite::{backup, Connection, ToSql};

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

    /// Optional file path to save the SQLite database
    #[arg(long, value_name = "FILE_PATH")]
    output_db: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let conn = Connection::open_in_memory()?;

    let (regular_files, stdin_tables) = process_file_arguments(&cli.files)?;

    // Attempt to load data.
    if let Err(e) = load_all_data(&conn, &regular_files, &stdin_tables) {
        // If loading fails, and we are not trying to save an (empty) DB, it's an error.
        // If --output-db was specified with no inputs, it's okay to proceed to save empty DB.
        if cli.output_db.is_none() || !regular_files.is_empty() || !stdin_tables.is_empty() {
            // Only return error if it's not the "save empty DB" case.
            return Err(format!("Failed to load data: {}", e).into());
        }
        // Warn if proceeding to save an empty DB after a load error with inputs.
        if !regular_files.is_empty() || !stdin_tables.is_empty() {
             eprintln!("Warning: Failed to load data: {}. Proceeding to save the database as requested.", e);
        }
    }

    let mut action_taken = false;

    if cli.schema {
        // Pass regular_files and stdin_tables for context to show_schemas
        show_schemas(&conn, &regular_files, &stdin_tables)?;
        action_taken = true;
    }

    // Determine the query source
    let mut query_from_stdin_attempted = false;
    let query_opt = cli.query.or(cli.query_after_delimiter).or_else(|| {
        if stdin_tables.is_empty() && atty::isnt(atty::Stream::Stdin) {
            query_from_stdin_attempted = true;
            let mut buffer = String::new();
            if io::stdin().read_to_string(&mut buffer).is_ok() && !buffer.trim().is_empty() {
                Some(buffer)
            } else {
                None
            }
        } else {
            None
        }
    });

    if let Some(query_str) = query_opt {
        if !query_str.trim().is_empty() {
            run_query(&conn, &query_str, &cli.format, cli.show_sql)?;
            action_taken = true;
        } else {
            // Handle empty query string case
            let is_saving_db = cli.output_db.is_some();
            let is_showing_schema = cli.schema;
            if !is_saving_db && !is_showing_schema { // Only error if no other action is pending
                if query_from_stdin_attempted {
                    eprintln!("Error: Received empty query from stdin.");
                } else {
                    eprintln!("Error: Empty query provided via command line argument.");
                }
                std::process::exit(1);
            } else if query_from_stdin_attempted { // Warn if empty query from stdin but other actions are pending
                eprintln!("Warning: Received empty query from stdin. Other actions (schema/output) will proceed.");
            } else { // Warn if empty query from args but other actions are pending
                 eprintln!("Warning: Empty query provided via command line argument. Other actions (schema/output) will proceed.");
            }
        }
    }

    if let Some(ref output_db_path) = cli.output_db {
        save_database(&conn, output_db_path)?;
        action_taken = true; // save_database already prints a success message
    }

    // Default action: if files were given (or stdin data expected) but no specific action
    // (query, schema flag, output_db) was taken, then show schema.
    if !action_taken && (!regular_files.is_empty() || !stdin_tables.is_empty()) {
        show_schemas(&conn, &regular_files, &stdin_tables)?;
        action_taken = true;
    }

    if !action_taken {
        // If execution reaches here and no arguments were initially passed (other than program name),
        // clap would typically show help. If args were passed but led to no action, it's an error.
        if std::env::args().len() > 1 || query_from_stdin_attempted {
            eprintln!("Error: No action performed. Specify a query, --schema, --output-db, or provide input files to see their schema by default.");
            eprintln!("Run with --help for usage information.");
            std::process::exit(1);
        } else {
            // This case implies the program was run with no arguments.
            // Let clap show the help message.
            // To explicitly show help:
            // Cli::command().print_help()?;
            // std::process::exit(0); // Or 1, depending on desired behavior for empty invocation
            // However, clap usually handles this automatically if `Cli::parse()` is called.
            // If not, and this state is reachable, it indicates a logic gap.
            // For now, assume clap handles it or a more specific error above should catch it.
        }
    }

    Ok(())
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
    conn: &Connection,
    files: &[PathBuf], 
    stdin_tables: &[(String, String)], 
) -> Result<(), Box<dyn Error>> {
    let mut tables_shown = false;
    // List all user tables from the connection
    let mut tbl_stmt = conn.prepare(
        "SELECT name
               FROM sqlite_master
              WHERE type='table'
                AND name NOT LIKE 'sqlite_%'
           ORDER BY name;", // Added ordering for consistency
    )?;
    let table_names = tbl_stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    if table_names.is_empty() {
        println!("No tables found in the database.");
        return Ok(());
    }

    // For each table, use PRAGMA table_info from the connection
    for table_name in table_names {
        println!("Table: {}", table_name);

        let pragma_sql = format!("PRAGMA table_info('{}')", table_name.replace("'", "''")); // Sanitize table name for SQL
        let mut col_stmt = conn.prepare(&pragma_sql)?;
        let columns = col_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?, // Column name
                row.get::<_, String>(2)?, // Column type
            ))
        })?;

        println!("Columns:");
        let mut found_columns = false;
        for col_result in columns {
            match col_result {
                Ok((name, typ)) => {
                    println!("  {} ({})", name, typ);
                    found_columns = true;
                }
                Err(e) => {
                    eprintln!("Error reading schema for column in table {}: {}", table_name, e);
                }
            }
        }
        if !found_columns {
            println!("  (No columns found or error reading columns)");
        }
        println!();
    }

    // Note: The original function had logic for reading schemas directly from CSV files
    // and for stdin if not yet loaded. If `load_all_data` preloads everything,
    // this simplified version focusing on `conn` should be sufficient.
    // If there's a need to show schema for files *not* loaded into the DB (e.g., before deciding to load),
    // that would require keeping some of the old logic and passing `files` and `stdin_tables`.
    // For this refactoring, we assume `load_all_data` has populated `conn`.

    // Add a check if no tables were found in the DB but files/stdin were specified,
    // it might indicate an issue or that they were not loadable (e.g. all non-DB/CSV files)
    if !tables_shown && (!files.is_empty() || !stdin_tables.is_empty()) {
        let mut has_potentially_loadable_inputs = false;
        for file in files {
            let ext = file.extension().unwrap_or_default();
            if ext == "csv" || ext == "sqlite" || ext == "db" {
                has_potentially_loadable_inputs = true;
                break;
            }
        }
        if !stdin_tables.is_empty() {
            has_potentially_loadable_inputs = true;
        }

        if has_potentially_loadable_inputs {
            println!("No tables found in the database. This might be due to errors during data loading or unsupported file types.");
        } else if !files.is_empty() {
            println!("No tables found in the database. The input files might not be supported types (CSV, SQLite).");
        } else {
             // This case (no tables shown, no files, no stdin_tables) should ideally not be hit
             // if `table_names.is_empty()` check at the beginning handles it.
             // If it is hit, it means table_names was not empty, but tables_shown remained false.
             // This implies an issue with iterating tables or PRAGMA.
        }
    } else if !tables_shown && files.is_empty() && stdin_tables.is_empty() {
        // This means `table_names.is_empty()` was true earlier and printed "No tables found..."
        // So no further message is needed here.
    }


    Ok(())
}


fn run_query(
    conn: &Connection,
    query: &str,
    format: &str,
    show_sql: bool,
) -> Result<(), Box<dyn Error>> {
    let sql = compile_prql(query)?;

    if show_sql {
        println!("{}", sql);
        return Ok(());
    }

    // Connection is now passed in, data loading is separate

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

fn save_database(source_conn: &Connection, output_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let mut dest_conn = Connection::open(output_path)?;
    let backup = backup::Backup::new(source_conn, &mut dest_conn)?;
    backup.run_to_completion(5, std::time::Duration::from_millis(250), None)?;
    eprintln!("Database saved to {}", output_path.display()); // Inform user
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
fn load_all_data(
    conn: &Connection,
    regular_files: &[PathBuf],
    stdin_tables: &[(String, String)],
) -> Result<(), Box<dyn Error>> {
    // Load regular files
    for file in regular_files {
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
            load_csv_with_polars(conn, &table_name.to_string(), file)?;
        }
    }

    // Load stdin data if needed
    if !stdin_tables.is_empty() { // Simpler check, actual data read happens next
        // Read stdin data once into memory only if there are stdin tables
        let mut stdin_data = Vec::new();
        // Only read if stdin is not a tty
        if atty::isnt(atty::Stream::Stdin) {
            io::stdin().read_to_end(&mut stdin_data)?;
        }
        
        if stdin_data.is_empty() && atty::isnt(atty::Stream::Stdin) {
             // This case means stdin was expected but was empty.
             // If stdin is a TTY, it's fine, it means no data was piped.
            return Err("Stdin tables specified, but no data received from stdin".into());
        }


        // Create each requested table from the same stdin data, if data was read
        if !stdin_data.is_empty() {
            for (table_name, _) in stdin_tables {
                load_csv_from_memory_with_polars(conn, table_name, &stdin_data)?;
            }
        } else if !stdin_tables.is_empty() && !atty::is(atty::Stream::Stdin) {
            // If stdin tables were specified, but stdin is a TTY and no data was piped,
            // it's not an error, but tables won't be loaded.
            // Or, if stdin was not a TTY but read_to_end somehow resulted in empty (e.g. Ctrl+D immediately).
            // This path implies stdin_tables is not empty, but stdin_data is.
            // Consider if this should be a warning or an error.
            // The original code errored if stdin_tables was non-empty but stdin was a TTY.
            // Let's stick to erroring if stdin tables are expected but not provided.
            // The check `atty::isnt(atty::Stream::Stdin)` handles the TTY case for reading.
            // If `stdin_data` is empty AND `stdin_tables` is not, it's an issue.
            // However, the `process_file_arguments` already tries to intelligently add stdin
            // only if it's not a TTY.
            // The crucial part is `stdin_data.is_empty()` after attempting a read.
            // If `stdin_tables` is populated, it means we expect data.
            // The logic in `process_file_arguments` for peeking might need adjustment
            // if it consumes the first byte, making `stdin_data` appear empty later.
            // For now, let's assume `read_to_end` gets everything if not a TTY.
            // If stdin_tables is not empty, and we are here, it means stdin was expected.
            // If stdin_data is empty, it's an error.
            // The `atty::isnt(atty::Stream::Stdin)` check before read should prevent reading from TTY.
            // If stdin_tables is not empty, and stdin is TTY, then `process_file_arguments`
            // should ideally not have added them, or `load_all_data` should not try to read.
            // The current logic: if stdin_tables is not empty, attempt read if not TTY.
            // If after that, data is empty, it's an error.
            // If stdin_tables is not empty and it IS a TTY, `stdin_data` will be empty.
            // This should be an error as per original logic.
             if atty::is(atty::Stream::Stdin) {
                return Err("Stdin tables specified, but stdin is a TTY. Pipe data to stdin.".into());
            }
            // If it wasn't a TTY but data is empty, it's also an error. (covered by the earlier check)
        }
    }
    Ok(())
}

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
