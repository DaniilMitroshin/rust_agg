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

        let value = self.data[self.index as usize]
            .get(fldname)
            .unwrap_or_else(|| panic!("Unknown field: {fldname}"));

        value
            .parse::<i32>()
            .unwrap_or_else(|_| panic!("Field {fldname} is not an integer field"))

    }

    fn get_string(&self, fldname: &str) -> String {

        self.data[self.index as usize]
            .get(fldname)
            .unwrap_or_else(|| panic!("Unknown field: {fldname}"))
            .to_string()

    }

}
