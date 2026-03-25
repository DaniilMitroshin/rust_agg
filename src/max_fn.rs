use crate::aggregation_fn::AggregationFn;

pub struct MaxFn {

    fldname: String,

    val: i32,

}

impl MaxFn {

    pub fn new(fldname: String) -> Self {

        MaxFn {

            fldname,

            val: i32::MIN,

        }

    }

}

impl AggregationFn for MaxFn {

    fn process_first(&mut self, value: i32) {

        self.val = value;

    }

    fn process_next(&mut self, value: i32) {

        if value > self.val {

            self.val = value;

        }

    }

    fn field_name(&self) -> String {

        format!("max_{}", self.fldname)

    }

    fn value(&self) -> i32 {

        self.val

    }

}
