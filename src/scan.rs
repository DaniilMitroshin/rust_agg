pub trait Scan {

    fn before_first(&mut self);

    fn next(&mut self) -> bool;

    fn get_int(&self, fldname: &str) -> i32;

    fn get_string(&self, fldname: &str) -> String;

}
