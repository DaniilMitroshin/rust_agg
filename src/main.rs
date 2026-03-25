mod scan;
mod aggregation_fn;

mod sum_fn;
mod min_fn;
mod max_fn;

mod group_by_scan;
mod group_value;

mod record;
mod record_scan;

use record::Record;
use record_scan::RecordScan;
use group_by_scan::GroupByScan;

fn main() {

    let data = vec![

        Record{
            dept:"IT".to_string(),
            salary:1000
        },

        Record{
            dept:"HR".to_string(),
            salary:800
        },

        Record{
            dept:"IT".to_string(),
            salary:1200
        },

        Record{
            dept:"HR".to_string(),
            salary:700
        },

        Record{
            dept:"IT".to_string(),
            salary:900
        }

    ];

    let mut scan = RecordScan::new(data);

    let mut group = GroupByScan::new(

        &mut scan,

        "dept".to_string(),

        "salary".to_string()

    );

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

}
