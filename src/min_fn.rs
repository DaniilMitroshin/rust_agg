use crate::aggregation_fn::AggregationFn;

pub struct MinFn {

    fldname: String,

    val: i32,

}

impl MinFn {

    pub fn new(fldname: String) -> Self {

        MinFn {

            fldname,

            val: i32::MAX,

        }

    }

}

impl AggregationFn for MinFn {

    fn process_first(&mut self, value: i32) {

        self.val = value;

    }

    fn process_next(&mut self, value: i32) {

        if value < self.val {

            self.val = value;

        }

    }

    fn field_name(&self) -> String {

        format!("min_{}", self.fldname)

    }

    fn value(&self) -> i32 {

        self.val

    }

}
