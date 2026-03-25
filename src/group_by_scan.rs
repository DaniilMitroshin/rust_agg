use std::collections::BTreeMap;

use crate::aggregation_fn::run_aggregations;
use crate::scan::Scan;
use crate::group_value::GroupValue;

pub struct GroupByScan {

    results: Vec<(GroupValue,Vec<i32>)>,

    groupfield: String,

    index: usize

}

impl GroupByScan {

    pub fn new(
        scan: &mut dyn Scan,
        groupfield: String,
        aggfield: String
    ) -> Self {

        let mut groups: BTreeMap<GroupValue, Vec<i32>> = BTreeMap::new();

        scan.before_first();

        while scan.next() {

            let mut key = GroupValue::new();

            key.add(groupfield.clone(), scan.get_string(&groupfield));

            let val = scan.get_int(&aggfield);

            groups
                .entry(key)
                .or_default()
                .push(val);

        }

        let mut results = Vec::new();

        for (k,v) in &groups {
            let aggregated = run_aggregations(v, &aggfield);

            results.push((
                k.clone(),
                vec![
                    *aggregated.get(&format!("sum_{aggfield}")).unwrap_or(&0),
                    *aggregated.get(&format!("min_{aggfield}")).unwrap_or(&0),
                    *aggregated.get(&format!("max_{aggfield}")).unwrap_or(&0),
                ],
            ));

        }

        GroupByScan {

            results,

            groupfield,

            index: 0

        }

    }

    pub fn before_first(&mut self) {

        self.index = 0;

    }

    pub fn next(&mut self) -> bool {

        if self.index < self.results.len() {

            self.index +=1;

            true

        } else {

            false

        }

    }

    pub fn get_group(&self) -> String {

        let current_group = &self.results[self.index - 1].0;

        current_group.get(&self.groupfield).unwrap_or("").to_string()

    }

    pub fn get_sum(&self) -> i32 {

        self.results[self.index-1].1[0]

    }

    pub fn get_min(&self) -> i32 {

        self.results[self.index-1].1[1]

    }

    pub fn get_max(&self) -> i32 {

        self.results[self.index-1].1[2]

    }

}
