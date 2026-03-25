use std::collections::BTreeMap;

use crate::max_fn::MaxFn;
use crate::min_fn::MinFn;
use crate::sum_fn::SumFn;

pub trait AggregationFn {

    fn process_first(&mut self, value: i32);

    fn process_next(&mut self, value: i32);

    fn field_name(&self) -> String;

    fn value(&self) -> i32;

}

pub fn build_aggregations(fldname: &str) -> Vec<Box<dyn AggregationFn>> {

    vec![
        Box::new(SumFn::new(fldname.to_string())),
        Box::new(MinFn::new(fldname.to_string())),
        Box::new(MaxFn::new(fldname.to_string())),
    ]

}

pub fn run_aggregations(values: &[i32], fldname: &str) -> BTreeMap<String, i32> {

    let mut result = BTreeMap::new();

    if values.is_empty() {
        return result;
    }

    let mut aggregations = build_aggregations(fldname);

    for aggregation in &mut aggregations {
        aggregation.process_first(values[0]);

        for value in &values[1..] {
            aggregation.process_next(*value);
        }

        result.insert(aggregation.field_name(), aggregation.value());
    }

    result

}
