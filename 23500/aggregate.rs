use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::hash::Hash;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group.
/// (You can add any other fields that you think are neccessary)
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped
/// by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    // I add hash_group to help me contain the vectors that is used to contain
    // the return tuple of the each group
    // hash_group_count is count the number of tuple that the origianl input
    // have for one group
    // hash_group_sum is to record the sum value of certain field in the return
    // tuple of a certain group
    // vec_group is contain the group names
    hash_group: HashMap<Vec<Field>, Vec<Field>>,
    hash_group_count: HashMap<Vec<Field>, usize>,
    hash_group_sum: HashMap<Vec<Field>, Vec<i32>>,
    vec_group: Vec<Vec<Field>>,
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields_: Vec<AggregateField>,
        groupby_field_: Vec<usize>,
        schema_: &TableSchema,
    ) -> Self {
        Aggregator {
            agg_fields: agg_fields_,
            groupby_fields: groupby_field_,
            schema: schema_.clone(),
            hash_group: HashMap::new(),
            hash_group_count: HashMap::new(),
            hash_group_sum: HashMap::new(),
            vec_group: Vec::new(),
        }
    }

    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        //we first take out the required group-by field in the tuple as the name
        //of this tuple
        let mut group_name = Vec::new();
        let max_group_field_index = self.groupby_fields.len();
        let mut group_field_index = 0;
        while group_field_index < max_group_field_index {
            let tuple_index_group = self.groupby_fields[group_field_index];
            let field = match tuple.get_field(tuple_index_group) {
                Some(f) => f.clone(),
                None => {
                    panic!("merge_tuple_into_group error 1, no such field");
                }
            };
            group_name.push(field);
            group_field_index += 1;
        }

        //then we take out the agg field from a tuple and prepare it to do some
        //further calculation to merge it back to its corresponding group
        let mut agg_field = Vec::new();
        let max_agg_field_index = self.agg_fields.len();
        let mut agg_field_index = 0;
        while agg_field_index < max_agg_field_index {
            let tuple_index_agg = self.agg_fields[agg_field_index].field;
            let field = match tuple.get_field(tuple_index_agg) {
                Some(f) => f.clone(),
                None => {
                    panic!("merge_tuple_into_group error 1, no such field");
                }
            };
            agg_field.push(field);
            agg_field_index += 1;
        }

        //we will check if there are such group exit in our return tuples;
        //if not such group we will use the new agg_field as the return group
        //tuple, and we will fill its information to the corresponding field.
        if !self.hash_group.contains_key(&group_name) {
            //if the tuple do avg operation, we need to calculate it, but we
            //do not need to any else for other operation
            let mut index_set_avg = 0;
            let max_index_set_avg = max_agg_field_index;
            let mut sum_no_group = Vec::new();
            while index_set_avg < max_index_set_avg {
                let set_avg_op = self.agg_fields[index_set_avg].op;
                let mut temp_sum = 0;
                match set_avg_op {
                    AggOp::Avg => {
                        let set_avg_field = agg_field[index_set_avg].clone();
                        match set_avg_field {
                            Field::IntField(i) => {
                                temp_sum = i.clone();
                            }
                            Field::StringField(a) => {
                                panic!("merge_tuple_into_group error 5");
                            }
                        }
                    }
                    // if it is count, we need to update its field to 1
                    AggOp::Count => {
                        agg_field[index_set_avg] = Field::IntField(1);
                    }
                    _ => (),
                }
                sum_no_group.push(temp_sum);
                index_set_avg += 1;
            }
            //add every back to the aggregator
            self.hash_group
                .insert(group_name.clone(), agg_field.clone());
            self.hash_group_count.insert(group_name.clone(), 1);
            self.hash_group_sum.insert(group_name.clone(), sum_no_group);
            self.vec_group.push(group_name.clone());
        }
        //if there is a existing accumulation the group, we will try to merge
        //the tuple with the existing one.
        else {
            //we take out the count, sum and exisitng tuple wait for further
            //operation
            let count = match self.hash_group_count.get(&group_name) {
                Some(c) => c.clone(),
                None => {
                    panic!("merge_tuple_into_group error 2");
                }
            };
            let mut sum = match self.hash_group_sum.get(&group_name) {
                Some(c) => c.clone(),
                None => {
                    panic!("merge_tuple_into_group error 3");
                }
            };
            let mut agg_in_group = match self.hash_group.get(&group_name) {
                Some(c) => c.clone(),
                None => {
                    panic!("merge_tuple_into_group error 4");
                }
            };
            let new_count = count.clone() + 1;
            let mut agg_field_index_2 = 0;
            // get into each field we need to do operation
            while agg_field_index_2 < max_agg_field_index {
                let operation = self.agg_fields[agg_field_index_2].op;
                let agg_in_group_field = agg_in_group[agg_field_index_2].clone();
                let agg_in_group_sum = sum[agg_field_index_2];
                let new_tuple_agg_field = agg_field[agg_field_index_2].clone();

                match operation {
                    //if it is avg, we will update sum and count and then
                    //calculate the avg
                    //if it is string, we will panic
                    AggOp::Avg => match agg_in_group_field {
                        Field::IntField(i) => {
                            let mut temp_field =
                                agg_in_group_sum + new_tuple_agg_field.unwrap_int_field();
                            let avg = (temp_field.clone() / new_count as i32) as i32;
                            agg_in_group[agg_field_index_2] = Field::IntField(avg);
                            sum[agg_field_index_2] = temp_field.clone();
                        }
                        Field::StringField(String) => {
                            panic!("string cannot avg merge_tuple_into_group");
                        }
                    },
                    //if operation is count, we will simply return the new
                    //count
                    AggOp::Count => {
                        agg_in_group[agg_field_index_2] = Field::IntField(new_count as i32);
                    }
                    //max will compare the existing value with the one from the
                    //new input and return the bigger one
                    AggOp::Max => match agg_in_group_field.clone() {
                        Field::IntField(i) => {
                            let mut temp_field_int = agg_in_group_field.clone().unwrap_int_field();
                            let mut temp_field_int2 = new_tuple_agg_field.unwrap_int_field();
                            agg_in_group[agg_field_index_2] =
                                Field::IntField(max(temp_field_int, temp_field_int2));
                        }
                        Field::StringField(String) => {
                            let mut temp_field_string3 =
                                agg_in_group_field.clone().unwrap_string_field().to_string();
                            let mut temp_field_string4 =
                                new_tuple_agg_field.unwrap_string_field().to_string();
                            agg_in_group[agg_field_index_2] =
                                Field::StringField(max(temp_field_string3, temp_field_string4));
                        }
                    },
                    //min will compare the existing value with the one from the
                    //new input and return the samller one
                    AggOp::Min => match agg_in_group_field.clone() {
                        Field::IntField(i) => {
                            let mut temp_field_int = agg_in_group_field.clone().unwrap_int_field();
                            let mut temp_field_int2 = new_tuple_agg_field.unwrap_int_field();
                            agg_in_group[agg_field_index_2] =
                                Field::IntField(min(temp_field_int, temp_field_int2));
                        }
                        Field::StringField(String) => {
                            let mut temp_field_string3 =
                                agg_in_group_field.clone().unwrap_string_field().to_string();
                            let mut temp_field_string4 =
                                new_tuple_agg_field.unwrap_string_field().to_string();
                            agg_in_group[agg_field_index_2] =
                                Field::StringField(min(temp_field_string3, temp_field_string4));
                        }
                    },
                    //sum will update the sum and if there is a string, it will panic
                    AggOp::Sum => match agg_in_group_field {
                        Field::IntField(i) => {
                            let mut new_sum = agg_in_group_field.clone().unwrap_int_field()
                                + new_tuple_agg_field.unwrap_int_field();
                            agg_in_group[agg_field_index_2] = Field::IntField(new_sum);
                        }
                        Field::StringField(String) => {
                            panic!("string cannot sum merge_tuple_into_group");
                        }
                    },
                }
                agg_field_index_2 += 1;
            }
            //finally fill everything back to the aggragater
            self.hash_group.insert(group_name.clone(), agg_in_group);
            self.hash_group_count.insert(group_name.clone(), new_count);
            self.hash_group_sum.insert(group_name.clone(), sum);
        };
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        //this will use the vec_group which contain all the name of the group
        //to read all the accumulating result tuple from the hashmap
        //then create a tuple iterator with those result tuple
        let mut new_vec_tuple = Vec::new();
        let vec_group_name = self.vec_group.clone();
        let mut index = 0;
        let max_index = vec_group_name.len();
        while index < max_index {
            let mut group_name = vec_group_name[index].clone();
            let mut new_group_vec = match self.hash_group.get(&vec_group_name[index]) {
                Some(v) => v.clone(),
                None => {
                    panic!("aggregate iterator error");
                }
            };
            group_name.append(&mut new_group_vec);
            let new_tuple = Tuple::new(group_name);
            new_vec_tuple.push(new_tuple);
            index += 1;
        }
        let result = TupleIterator::new(new_vec_tuple, self.schema.clone());
        return result;
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Fields to groupby over.
    groupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: Option<TupleIterator>,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        mut child_: Box<dyn OpIterator>,
    ) -> Self {
        //first take a tuple out to make sure the intput is not empty
        child_.open();
        match child_.next() {
            Ok(Some(a)) => (),
            Ok(None) => {
                return Aggregate {
                    groupby_fields: Vec::new(),
                    agg_fields: Vec::new(),
                    agg_iter: None,
                    schema: TableSchema::new(Vec::new()),
                    open: false,
                    child: child_,
                };
            }
            Err(a) => {
                panic!("aggregate new error 0")
            }
        }
        //find out the aggreagte field and store them to the vec_agg_field
        let mut vec_agg_field = Vec::new();
        let max_index_indices = agg_indices.len();
        let max_index_op = ops.len();
        let mut index_1 = 0;
        if max_index_indices == max_index_op {
            while index_1 < max_index_indices {
                let new_indices = agg_indices[index_1.clone()].clone();
                let new_op = ops[index_1].clone();
                let new_agg_field = AggregateField {
                    field: new_indices,
                    op: new_op,
                };
                vec_agg_field.push(new_agg_field.clone());
                index_1 += 1;
            }
        } else {
            panic!("the indices and the op has different length Aggregate 1");
        }

        //then we get schema and store them to the child_schema
        let child_schema = child_.get_schema().clone();
        let max_index_indices2 = groupby_indices.len();
        let mut index_2 = 0;
        let mut vec_attribute = Vec::new();
        while index_2 < max_index_indices2 {
            let mut new_attribute = match child_schema.get_attribute(index_2) {
                Some(a) => a.clone(),
                None => {
                    panic!("the indices and the op has different length Aggregate 2");
                }
            };
            new_attribute.name = groupby_names[index_2].to_string();
            vec_attribute.push(new_attribute);
            index_2 += 1;
        }
        //Then if the field is count we need to change the field type to int
        let max_index_indices3 = agg_indices.len();
        let mut index_3 = 0;
        while index_3 < max_index_indices3 {
            let mut new_attribute2 = match child_schema.get_attribute(index_3) {
                Some(a) => a.clone(),
                None => {
                    panic!("the indices and the op has different length Aggregate 2");
                }
            };
            new_attribute2.name = agg_names[index_3].to_string();
            match ops[index_3] {
                AggOp::Count => {
                    new_attribute2.dtype = DataType::Int;
                }
                _ => (),
            }
            vec_attribute.push(new_attribute2);
            index_3 += 1;
        }
        child_.rewind();
        //use the vec_attribute to build the table schema
        let table_schema = TableSchema::new(vec_attribute.clone());
        //and use the schema to generate a iterator struct
        let mut tuple_iter = Aggregator::new(
            vec_agg_field.clone(),
            groupby_indices.clone(),
            &table_schema,
        );
        //use the loop to take out tuple from children and insert them to
        //iterator
        loop {
            let tuple = match child_.next().unwrap() {
                Some(t) => t,
                None => {
                    break;
                }
            };
            tuple_iter.merge_tuple_into_group(&tuple);
        }
        //convert the iterator struct to a real iterator
        let mut option_tuple_it = tuple_iter.iterator();
        option_tuple_it.open();
        child_.close();
        //return the real iterator
        Aggregate {
            groupby_fields: groupby_indices,
            agg_fields: vec_agg_field,
            agg_iter: Some(option_tuple_it),
            schema: table_schema,
            open: false,
            child: child_,
        }
    }
}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        if self.open {
            panic!("Aggregate opiterator has been opened");
        }
        self.open = true;
        self.child.open();
        if self.open {
            return Ok(());
        } else {
            return Err(CrustyError::CrustyError(format!("Aggregate is not open")));
        }
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Aggregate opiterator has not been opened");
        }
        let mut iterator = match &self.agg_iter {
            Some(i) => i.clone(),
            None => {
                return Ok(None);
            }
        };
        let tuple = match iterator.next() {
            Ok(Some(t)) => Some(t),
            Ok(None) => None,
            Err(a) => {
                return Err(a);
            }
        };
        self.agg_iter = Some(iterator);
        Ok(tuple)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Aggregate opiterator has not been opened");
        }
        self.rewind();
        self.open = false;
        return Ok(());
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Aggregate opiterator has not been opened");
        }
        let mut new = match &self.agg_iter {
            Some(t) => t.clone(),
            None => {
                return Err(CrustyError::CrustyError(format!("aggregate has elements")));
            }
        };
        new.rewind();
        self.agg_iter = Some(new);
        return Ok(());
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "avg", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
