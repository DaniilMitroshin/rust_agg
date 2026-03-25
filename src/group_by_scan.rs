use std::collections::BTreeMap;

use crate::aggregation_fn::run_aggregations;
use crate::scan::Scan;
use crate::group_value::GroupValue;

pub struct GroupByScan {

    results: Vec<BTreeMap<String, String>>,

    headers: Vec<String>,

    index: usize

}

impl GroupByScan {

    pub fn new(
        scan: &mut dyn Scan,
        groupfields: Vec<String>,
        aggfields: Vec<String>
    ) -> Self {

        let mut groups: BTreeMap<GroupValue, BTreeMap<String, Vec<i32>>> = BTreeMap::new();

        scan.before_first();

        while scan.next() {

            let mut key = GroupValue::new();

            for groupfield in &groupfields {
                key.add(groupfield.clone(), scan.get_string(groupfield));
            }

            let entry = groups.entry(key).or_default();

            for aggfield in &aggfields {
                let val = scan.get_int(aggfield);
                entry.entry(aggfield.clone()).or_default().push(val);
            }

        }

        let mut results = Vec::new();
        let mut headers = groupfields.clone();

        for aggfield in &aggfields {
            headers.push(format!("sum_{aggfield}"));
            headers.push(format!("min_{aggfield}"));
            headers.push(format!("max_{aggfield}"));
        }

        for (group_value, grouped_fields) in &groups {
            let mut row = BTreeMap::new();

            for groupfield in &groupfields {
                row.insert(
                    groupfield.clone(),
                    group_value.get(groupfield).unwrap_or("").to_string(),
                );
            }

            for aggfield in &aggfields {
                let aggregated = grouped_fields
                    .get(aggfield)
                    .map(|values| run_aggregations(values, aggfield))
                    .unwrap_or_default();

                row.insert(
                    format!("sum_{aggfield}"),
                    aggregated
                        .get(&format!("sum_{aggfield}"))
                        .unwrap_or(&0)
                        .to_string(),
                );
                row.insert(
                    format!("min_{aggfield}"),
                    aggregated
                        .get(&format!("min_{aggfield}"))
                        .unwrap_or(&0)
                        .to_string(),
                );
                row.insert(
                    format!("max_{aggfield}"),
                    aggregated
                        .get(&format!("max_{aggfield}"))
                        .unwrap_or(&0)
                        .to_string(),
                );
            }

            results.push(row);

        }

        GroupByScan {

            results,

            headers,

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

    pub fn headers(&self) -> &[String] {

        &self.headers

    }

    pub fn get_value(&self, fldname: &str) -> String {

        self.results[self.index - 1]
            .get(fldname)
            .cloned()
            .unwrap_or_default()

    }

}
