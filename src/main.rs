use std::error::Error;
use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use csv::Reader;
use prql_compiler as prqlc;
use rusqlite::{Connection, ToSql};

/// A command-line tool to query CSV and SQLite files using PRQL (PRQL Query Language)
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a PRQL query against CSV and SQLite files
    Query {
        /// PRQL query string or path to .prql file
        #[arg(required = true)]
        query: String,

        /// File paths (CSV or SQLite, can specify multiple)
        #[arg(required = true)]
        files: Vec<PathBuf>,

        /// Output format (table, csv, json, logfmt)
        #[arg(short, long, default_value = "table", value_parser = ["table", "csv", "jsonl", "logfmt"])]
        format: String,
    },

    /// Show the resulting SQL without executing
    ShowSql {
        /// PRQL query string or path to .prql file
        query: String,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query {
            query,
            files,
            format,
        } => run_query(&query, &files, &format)?,
        Commands::ShowSql { query } => show_sql(&query)?,
    }

    Ok(())
}

fn run_query(query: &str, files: &[PathBuf], format: &str) -> Result<(), Box<dyn Error>> {
    let sql = compile_prql(query)?;

    let conn = Connection::open_in_memory()?;

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
            load_csv(&conn, &table_name, file)?;
        }
    }

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
    for row in &collected_rows {
        match format {
            "csv" => {
                let flat = row
                    .iter()
                    .map(|v| v.clone().unwrap_or_else(|| "NULL".into()))
                    .collect::<Vec<_>>();
                println!("{}", flat.join(","));
            }
            "jsonl" => {
                let json_obj: serde_json::Value = column_names
                    .iter()
                    .zip(row.iter())
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            serde_json::Value::String(
                                v.clone().unwrap_or_else(|| "null".into()),
                            ),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>()
                    .into();
                println!("{}", serde_json::to_string(&json_obj)?);
            }
            "logfmt" => {
                let mut line = String::new();
                for (k, v) in column_names.iter().zip(row.iter()) {
                    let val = v.clone().unwrap_or_else(|| "NULL".to_string());
                    line.push_str(&format!("{}=\"{}\" ", k, val.replace('"', "\\\"")));
                }
                println!("{}", line.trim_end());
            }
            "table" => {
                print_table(&column_names, &collected_rows);
                break; // already printed whole table; don't reprint each row
            }
            _ => {
                println!("{:?}", row);
            }
        }
    }
    Ok(())
}

fn show_sql(query: &str) -> Result<(), Box<dyn Error>> {
    let sql = compile_prql(query)?;
    println!("{}", sql);
    Ok(())
}

fn compile_prql(query: &str) -> Result<String, Box<dyn Error>> {
    if query.ends_with(".prql") {
        let prql = fs::read_to_string(query)?;
        Ok(prqlc::compile(&prql, &prqlc::Options::default())?)
    } else {
        Ok(prqlc::compile(query, &prqlc::Options::default())?)
    }
}

fn print_table(headers: &[String], rows: &[Vec<Option<String>>]) {
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
