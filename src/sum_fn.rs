use crate::aggregation_fn::AggregationFn;

pub struct SumFn {

    fldname: String,

    sum: i32,

}

impl SumFn {

    pub fn new(fldname: String) -> Self {

        SumFn {

            fldname,

            sum: 0,

        }

    }

}

impl AggregationFn for SumFn {

    fn process_first(&mut self, value: i32) {

        self.sum = value;

    }

    fn process_next(&mut self, value: i32) {

        self.sum += value;

    }

    fn field_name(&self) -> String {

        format!("sum_{}", self.fldname)

    }

    fn value(&self) -> i32 {

        self.sum

    }

}
