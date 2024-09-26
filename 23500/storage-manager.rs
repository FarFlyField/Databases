use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::delta_storage_trait::Value;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use core::num;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::{fs, path};

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: PathBuf,
    /// Map between ContainerID and its path.
    pub container_map: Arc<RwLock<HashMap<ContainerId, PathBuf>>>,

    #[serde(skip)]
    /// Not serialized: Map between a ContainerID and its shared HeapFile.
    pub(crate) heapfiles: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
    /// Indicates if this is a temp StorageManager (for testing)
    is_temp: bool,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        if !self.heapfiles.read().unwrap().contains_key(&container_id) {
            return None;
        }

        match self
            .heapfiles
            .read()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .read_page_from_file(page_id)
        {
            Ok(page) => {
                return Some(page);
            }
            Err(_) => {
                return None;
            }
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        if !self.heapfiles.read().unwrap().contains_key(&container_id) {
            return Err(CrustyError::ExecutionError(
                "write_page error: No container found.".to_string(),
            ));
        }

        let _write_res = match self
            .heapfiles
            .write()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .write_page_to_file(page)
        {
            Ok(()) => {
                return Ok(());
            }
            Err(error) => {
                return Err(error);
            }
        };
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        if !self.heapfiles.read().unwrap().contains_key(&container_id) {
            panic!("get_num_pages error: No container found.");
        }

        return self
            .heapfiles
            .read()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .num_pages();
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        if !self.heapfiles.read().unwrap().contains_key(&container_id) {
            (0, 0)
        } else {
            (
                self.heapfiles
                    .read()
                    .unwrap()
                    .get(&container_id)
                    .unwrap()
                    .read_count
                    .load(Ordering::Relaxed),
                self.heapfiles
                    .read()
                    .unwrap()
                    .get(&container_id)
                    .unwrap()
                    .write_count
                    .load(Ordering::Relaxed),
            )
        }
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }

    /// For testing
    pub fn get_page_bytes(&self, container_id: ContainerId, page_id: PageId) -> Vec<u8> {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => p.to_bytes(),
            None => Vec::new(),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_path for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_path: PathBuf) -> Self {
        if !storage_path.exists() {
            fs::create_dir_all(storage_path.clone())
                .expect("new_test_sm: Cannot create directory.");

            return StorageManager {
                storage_path,
                container_map: Arc::new(RwLock::new(HashMap::new())),
                heapfiles: Arc::new(RwLock::new(HashMap::new())),
                is_temp: false,
            };
        }

        let mut meta_path = PathBuf::clone(&storage_path);
        meta_path.push("metadata.json");

        if fs::read_dir(&storage_path).unwrap().count() == 0 || !meta_path.exists() {
            return StorageManager {
                storage_path,
                container_map: Arc::new(RwLock::new(HashMap::new())),
                heapfiles: Arc::new(RwLock::new(HashMap::new())),
                is_temp: false,
            };
        }

        let metafile = match fs::File::open(meta_path) {
            Ok(f) => f,
            Err(_) => panic!("new() error: Open file error."),
        };

        let mut new_sm: StorageManager = match serde_json::from_reader(metafile) {
            Ok(sm) => sm,
            Err(_) => panic!("new() error: Error in serde_json."),
        };

        let mut heapfiles_hm: HashMap<ContainerId, Arc<HeapFile>> = HashMap::new();

        for (id, path) in &*new_sm.container_map.read().unwrap() {
            if !path.exists() {
                panic!("Container at {} does not exist.", path.to_string_lossy());
            }

            let new_hf = match HeapFile::new(path.clone(), *id) {
                Ok(f) => f,
                Err(_) => panic!("new() error: Unable to create HeapFile struct."),
            };

            heapfiles_hm.insert(*id, Arc::new(new_hf));
        }

        new_sm.heapfiles = Arc::new(RwLock::new(heapfiles_hm));

        return new_sm;
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_path = gen_random_test_sm_dir();
        println!("Making new temp storage_manager {:?}", storage_path);
        fs::create_dir_all(storage_path.clone()).expect("new_test_sm: Cannot create directory.");

        StorageManager {
            storage_path,
            container_map: Arc::new(RwLock::new(HashMap::new())),
            heapfiles: Arc::new(RwLock::new(HashMap::new())),
            is_temp: true,
        }
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        if !self
            .container_map
            .read()
            .unwrap()
            .contains_key(&container_id)
        {
            panic!("insert_value error: Invalid container_id!");
        }

        let num_pages = self.get_num_pages(container_id);
        let file_path = self
            .container_map
            .read()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .clone();

        for i in 0..num_pages {
            let mut curr_page =
                match self.get_page(container_id, i, tid, Permissions::ReadWrite, false) {
                    Some(p) => p,
                    None => panic!("insert_value error: Get page error."),
                };

            let add_res = curr_page.add_value(&value);
            if add_res == None {
                continue;
            }

            let curr_page_off = self
                .heapfiles
                .read()
                .unwrap()
                .get(&container_id)
                .unwrap()
                .page_offsets
                .read()
                .unwrap()
                .get(&i)
                .unwrap()
                .clone();

            let mut open_file = match fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(file_path)
            {
                Ok(f) => f,
                Err(_) => panic!("insert_value error: Open file error."),
            };

            let _seek_pos = open_file
                .seek(SeekFrom::Start(curr_page_off as u64))
                .unwrap();
            let _write_res = open_file.write(&curr_page.to_bytes());

            let value_id = ValueId {
                container_id,
                page_id: Some(i),
                slot_id: Some(add_res.unwrap()),
                segment_id: None,
            };

            return value_id;
        }

        let mut new_page = Page::new(num_pages);
        let slot_id = new_page.add_value(&value).unwrap();

        match self.write_page(container_id, new_page, tid) {
            Ok(()) => {
                return ValueId {
                    container_id,
                    page_id: Some(num_pages),
                    slot_id: Some(slot_id),
                    segment_id: None,
                };
            }
            Err(_) => panic!("insert_value error: write_page error."),
        }
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let container_id = id.container_id;
        let page_id = match id.page_id {
            Some(id) => id,
            None => {
                return Err(CrustyError::CrustyError(
                    "delete_value error: ValueId no page_id.".to_string(),
                ));
            }
        };
        let slot_id = match id.slot_id {
            Some(id) => id,
            None => {
                return Err(CrustyError::CrustyError(
                    "delete_value error: ValueId no slot_id.".to_string(),
                ));
            }
        };

        if !self
            .container_map
            .read()
            .unwrap()
            .contains_key(&container_id)
        {
            return Err(CrustyError::CrustyError(format!(
                "delete_value error: Container
            with id {} does not exist.",
                container_id
            )));
        }

        let file_path = self
            .container_map
            .read()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .clone();
        let mut curr_page =
            match self.get_page(container_id, page_id, tid, Permissions::ReadWrite, false) {
                Some(p) => p,
                None => {
                    return Err(CrustyError::ExecutionError(
                        "delete_value error: get_page error.".to_string(),
                    ));
                }
            };

        curr_page
            .delete_value(slot_id)
            .expect("delete_value error: delete_value for page error.");
        let curr_page_off = self
            .heapfiles
            .read()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .page_offsets
            .read()
            .unwrap()
            .get(&page_id)
            .unwrap()
            .clone();

        let mut open_file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
        {
            Ok(f) => f,
            Err(_) => panic!("delete_value error: Open file error."),
        };

        let _seek_pos = open_file
            .seek(SeekFrom::Start(curr_page_off as u64))
            .unwrap();
        let _write_res = open_file.write(&curr_page.to_bytes());

        Ok(())
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        let container_id = id.container_id;

        self.delete_value(id, _tid)
            .expect_err("update_value error: Error in delete_value");

        Ok(self.insert_value(container_id, value, _tid))
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        if self
            .container_map
            .read()
            .unwrap()
            .contains_key(&container_id)
        {
            return Err(CrustyError::CrustyError
                (format!("create_container error: Container with id {} exists.", container_id)));
            // self.remove_container(container_id).unwrap();
        }

        let mut hf_path = PathBuf::from(self.storage_path.clone());
        hf_path.push(container_id.to_string());
        hf_path.set_extension("hs");

        let _open_file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(hf_path.clone())
        {
            Ok(f) => f,
            Err(error) => {
                return Err(error.into());
            }
        };

        self.container_map
            .write()
            .unwrap()
            .insert(container_id, hf_path.clone());

        let new_hf = match HeapFile::new(hf_path, container_id) {
            Ok(hf) => hf,
            Err(error) => {
                return Err(error);
            }
        };

        self.heapfiles
            .write()
            .unwrap()
            .insert(container_id, Arc::new(new_hf));

        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        if !self
            .container_map
            .read()
            .unwrap()
            .contains_key(&container_id)
        {
            return Err(CrustyError::CrustyError(format!(
                "remove_container error: Container
            with id {} does not exist.",
                container_id
            )));
        }

        let _remove = match fs::remove_file(
            self.container_map
                .read()
                .unwrap()
                .get(&container_id)
                .unwrap(),
        ) {
            Ok(()) => (),
            Err(error) => {
                return Err(CrustyError::IOError(format!(
                    "Error in removing file {} {} {:?}",
                    self.container_map
                        .read()
                        .unwrap()
                        .get(&container_id)
                        .unwrap()
                        .to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

        self.container_map.write().unwrap().remove(&container_id);
        self.heapfiles.write().unwrap().remove(&container_id);

        Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        if !self.heapfiles.read().unwrap().contains_key(&container_id) {
            panic!("get_iterator error: No such container.");
        }
        let heapfile = match self.heapfiles.read().unwrap().get(&container_id) {
            // Using self written clone function.
            Some(hp) => hp.clone(),
            None => {
                panic!("get_iterator error: Cannot get heapfile.");
            }
        };

        let iterator: HeapFileIterator = HeapFileIterator::new(tid, heapfile);
        iterator
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let container_id = id.container_id;
        let page_id = match id.page_id {
            Some(id) => id,
            None => {
                return Err(CrustyError::CrustyError(format!(
                    "get_value error: No page_id."
                )));
            }
        };
        let slot_id = match id.slot_id {
            Some(id) => id,
            None => {
                return Err(CrustyError::CrustyError(format!(
                    "get_value error: No slot_id."
                )));
            }
        };

        let page = match self.get_page(container_id, page_id, tid, perm, true) {
            Some(p) => p,
            None => {
                return Err(CrustyError::ExecutionError(format!(
                    "get_value error: get_page error."
                )));
            }
        };

        let get_data = match page.get_value(slot_id) {
            Some(data) => data,
            None => {
                return Err(CrustyError::IOError(format!(
                    "get_value error: get_value from page error."
                )));
            }
        };

        Ok(get_data)
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_path.clone())?;
        fs::create_dir_all(self.storage_path.clone()).unwrap();

        self.container_map.write().unwrap().clear();
        self.heapfiles.write().unwrap().clear();

        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {}

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        let json_string = match serde_json::to_string(self) {
            Ok(string) => string,
            Err(_) => panic!("shutdown: Error occurred at serialization"),
        };

        let mut path_to_meta = PathBuf::clone(&self.storage_path);
        path_to_meta.push("metadata");
        path_to_meta.set_extension("json");

        let mut file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path_to_meta)
        {
            Ok(f) => f,
            Err(_) => panic!("shutdown: Error occurred on create/open file."),
        };

        let _write_res = match file.write_all(json_string.as_bytes()) {
            Ok(_) => (),
            Err(_) => panic!("shutdown: Error occurred on write file."),
        };
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Err(CrustyError::CrustyError(String::from("TODO")))
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.to_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_path);
            let remove_all = fs::remove_dir_all(self.storage_path.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]

    // fn test_serde()
    // {
    //     init();
    //     let path = PathBuf::from("/Users/qcyang/code/CMSC23500/crustydb-Polymerase-2/src/heapstore/src");
    //     let mut map: HashMap<ContainerId, PathBuf> = HashMap::new();
    //     map.insert(0, PathBuf::from("Test for 0"));
    //     map.insert(1, PathBuf::from("Test for 1"));

    //     let map2: HashMap<ContainerId, Arc<HeapFile>> = HashMap::new();
    //     let test_sm = StorageManager {storage_path: path, container_map: Arc::new(RwLock::new(map)), heapfiles: Arc::new(RwLock::new(map2)), is_temp: false};
    //     test_sm.shutdown();
    //     test_sm.shutdown();
    //     test_sm.shutdown();

    //     let file = fs::File::open("/Users/qcyang/code/CMSC23500/crustydb-Polymerase-2/src/heapstore/src/metadata.json").unwrap();
    //     let new_test: StorageManager = serde_json::from_reader(file).unwrap();
    //     println!("{:?}", new_test.container_map.read().unwrap());
    // }

    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
