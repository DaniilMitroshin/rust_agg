use std::collections::BTreeMap;

#[derive(Clone)]
pub struct Record {
    pub fields: BTreeMap<String, String>,
}

impl Record {

    pub fn new(fields: BTreeMap<String, String>) -> Self {

        Record { fields }

    }

    pub fn get(&self, fldname: &str) -> Option<&str> {

        self.fields.get(fldname).map(String::as_str)

    }

}
