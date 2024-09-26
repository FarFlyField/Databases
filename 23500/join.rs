use super::{OpIterator, TupleIterator};
use common::{CrustyError, Field, SimplePredicateOp, TableSchema, Tuple};
use std::collections::HashMap;

/// Compares the fields of two tuples using a predicate. (You can add any other fields that you think are neccessary)
pub struct JoinPredicate {
    /// Operation to comapre the fields with.
    op: SimplePredicateOp,
    /// Index of the field of the left table (tuple).
    left_index: usize,
    /// Index of the field of the right table (tuple).
    right_index: usize,
}

impl JoinPredicate {
    /// Constructor that determines if two tuples satisfy the join condition.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation to compare the two fields with.
    /// * `left_index` - Index of the field to compare in the left tuple.
    /// * `right_index` - Index of the field to compare in the right tuple.
    fn new(op_: SimplePredicateOp, left_index_: usize, right_index_: usize) -> Self {
        JoinPredicate {
            op: op_,
            left_index: left_index_,
            right_index: right_index_,
        }
    }
}

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left_child: Box<dyn OpIterator>,
    /// Right child node.
    right_child: Box<dyn OpIterator>,
    /// Schema of the result.
    schema: TableSchema,
    open: bool,
    left_tuple: Option<Tuple>,
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op_: SimplePredicateOp,
        left_index_: usize,
        right_index_: usize,
        left_child_: Box<dyn OpIterator>,
        right_child_: Box<dyn OpIterator>,
        open: bool,
    ) -> Self {
        Join {
            predicate: JoinPredicate::new(op_, left_index_, right_index_),
            schema: left_child_.get_schema().merge(right_child_.get_schema()),
            left_child: left_child_,
            right_child: right_child_,
            open: false,
            left_tuple: None,
        }
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        if self.open {
            panic!("Aggregate opiterator has not been opened");
        }
        self.open = true;
        self.left_child.open();
        self.right_child.open();
        self.left_tuple = self.left_child.next()?;
        Ok(())
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("join opiterator has not been opened")
        }
        //we use two loop to control the iteration of the two table
        loop {
            //first take one tuple out from the left side
            let left = match &self.left_tuple {
                Some(l) => l.clone(),
                None => {
                    break;
                }
            };
            //iterate all the tuple on the right and compare if it satisfy
            //the operation requiremnet
            loop {
                let right = match self.right_child.next() {
                    Ok(Some(a)) => a,
                    Ok(None) => {
                        break;
                    }
                    Err(a) => {
                        panic!("join next error");
                    }
                };
                //do the comparing
                let reuslt = self.predicate.op.compare(
                    &left.get_field(self.predicate.left_index).unwrap(),
                    &right.get_field(self.predicate.right_index).unwrap(),
                );
                if reuslt {
                    return Ok(Some(left.merge(&right)));
                }
            }
            //if there is no satisfactory one from the right side, we
            //will take the next one from the right and do a new iteration
            //from the right
            self.left_tuple = match self.left_child.next() {
                Ok(Some(t)) => Some(t),
                Ok(None) => {
                    break;
                }
                Err(a) => {
                    panic!("join next error2");
                }
            };
            match self.right_child.rewind() {
                Ok(a) => (),
                Err(a) => {
                    panic!("join next error3");
                }
            };
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("join opiterator has not been opened")
        }
        match self.left_child.close() {
            Ok(a) => (),
            Err(a) => {
                panic!("join close error3");
            }
        };
        match self.right_child.close() {
            Ok(a) => (),
            Err(a) => {
                panic!("join close error3");
            }
        };
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("join opiterator has not been opened")
        }
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.close()?;
        self.open()
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    predicate: JoinPredicate,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    open: bool,
    schema: TableSchema,
    hash: HashMap<Field, Vec<usize>>,
    right_tuples: Vec<Tuple>,
    left: Option<Tuple>,
    index: usize,
}

impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child_: Box<dyn OpIterator>,
        right_child_: Box<dyn OpIterator>,
    ) -> Self {
        HashEqJoin {
            predicate: JoinPredicate::new(op, left_index, right_index),
            open: false,
            schema: left_child_.get_schema().merge(right_child_.get_schema()),
            left_child: left_child_,
            right_child: right_child_,
            right_tuples: Vec::new(),
            hash: HashMap::new(),
            left: None,
            index: 0,
        }
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        if self.open {
            panic!("Operator has not been opened")
        }
        self.open = true;
        self.left_child.open();
        self.right_child.open();
        let mut count = 0;
        //we will use a loop to take out all the field and insert their index
        //back to the hash map
        //we will use the right_tuple vec to store all the right tuples that
        while let Some(right) = self.right_child.next()? {
            //take out the field as the key to find a tuple
            let field = right.get_field(self.predicate.right_index).unwrap();
            //if there exist such a field we add the new tuple index to
            //the vec we store in the hash. This is for us to look for
            //it when we use the right_tuples
            if let Some(vec) = self.hash.get_mut(field) {
                vec.push(count);
            } else {
                //if there is not such a field in the hashmap, we will add a new
                //one to the hashmap
                self.hash.insert(field.clone(), vec![count]);
            }
            self.right_tuples.push(right);
            count += 1;
        }
        //then get the left-child ready
        self.left = self.left_child.next()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        //if the left child is available, then try to take its target field
        let mut field;
        let mut index;
        while let Some(left) = &self.left {
            field = left.get_field(self.predicate.left_index).unwrap();
            //if the target field also exist in the hash field, the try to
            //join the left tuple to the right one store in the right_tuples
            if let Some(vec) = self.hash.get(field) {
                if vec.len() > self.index {
                    index = self.index;
                    self.index += 1;
                    return Ok(Some(left.merge(&self.right_tuples[vec[index]])));
                }
            }
            //get new value for the left value
            self.left = self.left_child.next()?;
            self.index = 0;
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        self.left_child.close()?;
        self.right_child.close()?;
        self.right_tuples = Vec::new();
        self.index = 0;
        self.hash = HashMap::new();
        self.left = None;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.close()?;
        self.open()
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;
    use common::testutil::*;

    const WIDTH1: usize = 2;
    const WIDTH2: usize = 3;
    enum JoinType {
        NestedLoop,
        HashEq,
    }

    pub fn scan1() -> TupleIterator {
        let tuples = create_tuple_list(vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]);
        let ts = get_int_table_schema(WIDTH1);
        TupleIterator::new(tuples, ts)
    }

    pub fn scan2() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
            vec![4, 5, 6],
            vec![5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3],
            vec![3, 4, 3, 4, 5],
            vec![5, 6, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn gt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![3, 4, 1, 2, 3], // 1, 2 < 3
            vec![3, 4, 2, 3, 4],
            vec![5, 6, 1, 2, 3], // 1, 2, 3, 4 < 5
            vec![5, 6, 2, 3, 4],
            vec![5, 6, 3, 4, 5],
            vec![5, 6, 4, 5, 6],
            vec![7, 8, 1, 2, 3], // 1, 2, 3, 4, 5 < 7
            vec![7, 8, 2, 3, 4],
            vec![7, 8, 3, 4, 5],
            vec![7, 8, 4, 5, 6],
            vec![7, 8, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 2, 3, 4], // 1 < 2, 3, 4, 5
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 4, 5, 6], // 3 < 4, 5
            vec![3, 4, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_or_eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3], // 1 <= 1, 2, 3, 4, 5
            vec![1, 2, 2, 3, 4],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 3, 4, 5], // 3 <= 3, 4, 5
            vec![3, 4, 4, 5, 6],
            vec![3, 4, 5, 6, 7],
            vec![5, 6, 5, 6, 7], // 5 <= 5
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    fn construct_join(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
    ) -> Box<dyn OpIterator> {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        match ty {
            JoinType::NestedLoop => Box::new(Join::new(op, left_index, right_index, s1, s2, false)),
            JoinType::HashEq => Box::new(HashEqJoin::new(op, left_index, right_index, s1, s2)),
        }
    }

    fn test_get_schema(join_type: JoinType) {
        let op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let expected = get_int_table_schema(WIDTH1 + WIDTH2);
        let actual = op.get_schema();
        assert_eq!(&expected, actual);
    }

    fn test_next_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.next().unwrap();
    }

    fn test_close_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.close().unwrap();
    }

    fn test_rewind_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.rewind().unwrap();
    }

    fn test_rewind(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.open()?;
        while op.next()?.is_some() {}
        op.rewind()?;

        let mut eq_join = eq_join();
        eq_join.open()?;

        let acutal = op.next()?;
        let expected = eq_join.next()?;
        assert_eq!(acutal, expected);
        Ok(())
    }

    fn test_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let mut eq_join = eq_join();
        op.open()?;
        eq_join.open()?;
        match_all_tuples(op, Box::new(eq_join))
    }

    fn test_gt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::GreaterThan, 0, 0);
        let mut gt_join = gt_join();
        op.open()?;
        gt_join.open()?;
        match_all_tuples(op, Box::new(gt_join))
    }

    fn test_lt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThan, 0, 0);
        let mut lt_join = lt_join();
        op.open()?;
        lt_join.open()?;
        match_all_tuples(op, Box::new(lt_join))
    }

    fn test_lt_or_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThanOrEq, 0, 0);
        let mut lt_or_eq_join = lt_or_eq_join();
        op.open()?;
        lt_or_eq_join.open()?;
        match_all_tuples(op, Box::new(lt_or_eq_join))
    }

    mod join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn close_not_open() {
            test_close_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::NestedLoop);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::NestedLoop)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::NestedLoop)
        }

        #[test]
        fn gt_join() -> Result<(), CrustyError> {
            test_gt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_join() -> Result<(), CrustyError> {
            test_lt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_or_eq_join() -> Result<(), CrustyError> {
            test_lt_or_eq_join(JoinType::NestedLoop)
        }
    }

    mod hash_join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::HashEq);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::HashEq)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::HashEq)
        }
    }
}
