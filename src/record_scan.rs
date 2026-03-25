use crate::scan::Scan;
use crate::record::Record;

pub struct RecordScan {

    data: Vec<Record>,

    index: i32

}

impl RecordScan {

    pub fn new(data: Vec<Record>) -> Self {

        RecordScan {

            data,

            index: -1

        }

    }

}

impl Scan for RecordScan {

    fn before_first(&mut self) {

        self.index = -1;

    }

    fn next(&mut self) -> bool {

        self.index +=1;

        (self.index as usize) < self.data.len()

    }

    fn get_int(&self, fldname: &str) -> i32 {

        match fldname {
            "salary" => self.data[self.index as usize].salary,
            _ => panic!("Field {fldname} is not an integer field"),
        }

    }

    fn get_string(&self, fldname: &str) -> String {

        match fldname {
            "dept" => self.data[self.index as usize].dept.clone(),
            _ => panic!("Field {fldname} is not a string field"),
        }

    }

}
