mod scan;
mod aggregation_fn;

mod sum_fn;
mod min_fn;
mod max_fn;

mod group_by_scan;
mod group_value;

mod record;
mod record_scan;

use std::env;
use std::error::Error;
use std::fs;
use std::collections::BTreeMap;

use record::Record;
use record_scan::RecordScan;
use group_by_scan::GroupByScan;

fn parse_csv_line(line: &str) -> Vec<String> {

    line.split(',').map(|part| part.trim().to_string()).collect()

}

fn load_records_from_csv(path: &str) -> Result<(Vec<String>, Vec<Record>), Box<dyn Error>> {

    let content = fs::read_to_string(path)?;
    let mut lines = content.lines().filter(|line| !line.trim().is_empty());
    let headers_line = lines
        .next()
        .ok_or_else(|| "CSV file is empty".to_string())?;
    let headers = parse_csv_line(headers_line);

    if headers.is_empty() {
        return Err("CSV header is empty".into());
    }

    let mut data = Vec::new();

    for (line_offset, line) in lines.enumerate() {
        let trimmed = line.trim();
        let parts = parse_csv_line(trimmed);

        if parts.len() != headers.len() {
            return Err(format!("Invalid CSV line {}: {}", line_offset + 2, line).into());
        }

        let mut fields = BTreeMap::new();

        for (header, value) in headers.iter().zip(parts.iter()) {
            fields.insert(header.clone(), value.clone());
        }

        data.push(Record::new(fields));
    }

    Ok((headers, data))

}

fn print_table(headers: &[String], rows: &[Vec<String>]) {

    println!("{}", headers.join(" "));

    for row in rows {
        println!("{}", row.join(" "));
    }

    println!();

}

fn print_raw_data(headers: &[String], data: &[Record]) {

    let rows: Vec<Vec<String>> = data
        .iter()
        .map(|record| {
            headers
                .iter()
                .map(|header| record.get(header).unwrap_or("").to_string())
                .collect()
        })
        .collect();

    println!("Raw data:");
    print_table(headers, &rows);

}

fn parse_column_list(value: &str) -> Vec<String> {

    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()

}

fn infer_numeric_columns(headers: &[String], data: &[Record], excluded: &[String]) -> Vec<String> {

    headers
        .iter()
        .filter(|header| !excluded.contains(header))
        .filter(|header| {
            data.iter().all(|record| {
                record
                    .get(header)
                    .is_some_and(|value| value.parse::<i32>().is_ok())
            })
        })
        .cloned()
        .collect()

}

fn validate_columns(
    headers: &[String],
    data: &[Record],
    group_columns: &[String],
    aggregate_columns: &[String],
) -> Result<(), Box<dyn Error>> {

    for column in group_columns.iter().chain(aggregate_columns.iter()) {
        if !headers.contains(column) {
            return Err(format!("Column not found in CSV: {column}").into());
        }
    }

    for column in aggregate_columns {
        if data.iter().any(|record| {
            record
                .get(column)
                .is_none_or(|value| value.parse::<i32>().is_err())
        }) {
            return Err(format!("Aggregate column must contain only integers: {column}").into());
        }
    }

    Ok(())

}

fn main() -> Result<(), Box<dyn Error>> {

    let args: Vec<String> = env::args().collect();
    let csv_path = args.get(1).cloned().unwrap_or_else(|| "data.csv".to_string());
    let user_group_columns = args.get(2).map(|value| parse_column_list(value));
    let user_aggregate_columns = args.get(3).map(|value| parse_column_list(value));

    let (headers, data) = load_records_from_csv(&csv_path)?;

    let group_columns = user_group_columns.unwrap_or_else(|| {
        headers
            .first()
            .cloned()
            .into_iter()
            .collect()
    });

    let aggregate_columns = user_aggregate_columns.unwrap_or_else(|| {
        infer_numeric_columns(&headers, &data, &group_columns)
    });

    if group_columns.is_empty() {
        return Err("No group by columns provided".into());
    }

    if aggregate_columns.is_empty() {
        return Err("No numeric aggregate columns found".into());
    }

    validate_columns(&headers, &data, &group_columns, &aggregate_columns)?;

    print_raw_data(&headers, &data);

    let mut scan = RecordScan::new(data);

    let mut group = GroupByScan::new(

        &mut scan,

        group_columns.clone(),

        aggregate_columns.clone()

    );

    println!("Group by result:");
    print_table(group.headers(), &[]);

    group.before_first();
    while group.next() {
        let row: Vec<String> = group
            .headers()
            .iter()
            .map(|header| group.get_value(header))
            .collect();
        println!("{}", row.join(" "));
    }

    println!();

    Ok(())

}
