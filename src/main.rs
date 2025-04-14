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

        /// Output format (csv, table)
        #[arg(short, long, default_value = "table")]
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
    let rows = stmt.query_map([], |row| {
        Ok((0..row.as_ref().column_count())
            .map(|i| match row.get_ref(i).unwrap() {
                rusqlite::types::ValueRef::Null => "NULL".to_string(),
                rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                rusqlite::types::ValueRef::Real(f) => f.to_string(),
                rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
                rusqlite::types::ValueRef::Blob(_) => "[BLOB]".to_string(),
            })
            .collect::<Vec<String>>())
    })?;

    for row in rows {
        let row = row?;
        if format == "csv" {
            println!("{}", row.join(","));
        } else {
            println!("{:?}", row);
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
