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

use record::Record;
use record_scan::RecordScan;
use group_by_scan::GroupByScan;

fn load_records_from_csv(path: &str) -> Result<Vec<Record>, Box<dyn Error>> {

    let content = fs::read_to_string(path)?;
    let mut data = Vec::new();

    for (line_number, line) in content.lines().enumerate() {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            continue;
        }

        if line_number == 0 && trimmed.eq_ignore_ascii_case("dept,salary") {
            continue;
        }

        let parts: Vec<&str> = trimmed.split(',').map(str::trim).collect();

        if parts.len() != 2 {
            return Err(format!("Invalid CSV line {}: {}", line_number + 1, line).into());
        }

        let salary = parts[1]
            .parse::<i32>()
            .map_err(|_| format!("Invalid salary on line {}: {}", line_number + 1, parts[1]))?;

        data.push(Record {
            dept: parts[0].to_string(),
            salary,
        });
    }

    Ok(data)

}

fn print_raw_data(data: &[Record]) {

    println!("Raw data:");
    println!("dept salary");

    for record in data {
        println!("{} {}", record.dept, record.salary);
    }

    println!();

}

fn main() -> Result<(), Box<dyn Error>> {

    let csv_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "data.csv".to_string());

    let data = load_records_from_csv(&csv_path)?;

    print_raw_data(&data);

    let mut scan = RecordScan::new(data);

    let mut group = GroupByScan::new(

        &mut scan,

        "dept".to_string(),

        "salary".to_string()

    );

    println!("Group by result:");
    println!("Dept SUM MIN MAX");

    while group.next(){

        println!(
            "{} {} {} {}",
            group.get_group(),
            group.get_sum(),
            group.get_min(),
            group.get_max()
        );

    }

    Ok(())

}
