#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupValue {

    pub vals: Vec<(String,String)>

}

impl GroupValue {

    pub fn new() -> Self {

        GroupValue {

            vals: Vec::new()

        }

    }

    pub fn add(&mut self, fldname: String, value: String) {

        self.vals.push((fldname, value));

    }

    pub fn get(&self, fldname: &str) -> Option<&str> {

        self.vals
            .iter()
            .find(|(name, _)| name == fldname)
            .map(|(_, value)| value.as_str())

    }

}
