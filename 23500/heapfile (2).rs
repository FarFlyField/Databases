use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};

//My own implementation
use std::env;
use std::fs;
use std::collections::HashMap;

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, 
/// as it needs to maintain a link to a
/// File object, 
/// which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    //TODO milestone hs
    pub num_of_pages: Arc<RwLock<PageId>>, 
    pub file_name: Arc<RwLock<PathBuf>>,
    pub page_id_index_hm: Arc<RwLock<HashMap<PageId,usize>>>,
    //a pointer to the file
    pub file_ptr: fs::File, 
    // Track this HeapFile's container Id
    pub container_id: ContainerId,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))

            }
        };

        //TODO milestone hs
        //the file path here is already a .hf path
        //if the file already has some pages in there, I just rebuild the heapfile from it. 
        let metadata = match fs::metadata(&file_path)
        {
            Ok(meta) => meta,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

        let page_num = metadata.len()/4096;

        if page_num == 0 {
            //a totally new HeapFile
            let new_hashmap:HashMap<PageId,usize> = HashMap::new();
         
            Ok(HeapFile {
                //TODO milestone hs
                num_of_pages: Arc::new(RwLock::new(0)), 
                file_name: Arc::new(RwLock::new(file_path)),
                page_id_index_hm: Arc::new(RwLock::new(new_hashmap)),
                file_ptr: file,
                container_id,
                read_count: AtomicU16::new(0),
                write_count: AtomicU16::new(0),
            })
        }
        else
        {
            //prepare a hashmap for input later
            let mut old_hashmap :HashMap<PageId,usize> = HashMap::new();
            for i in 0..page_num{
                //prepare a buffer for page Id
                let mut page_id_buf = [0;2];
                let index_now = i*4096;
                let _f = match file.seek(SeekFrom::Start(index_now)){
                    Ok(_file) => _file,
                    Err(error) => {
                        return Err(CrustyError::CrustyError(format!(
                            "Cannot open or create heap file: {} {} {:?}",
                            file_path.to_string_lossy(),
                            error.to_string(),
                            error
                        )))
                    }
                };
                
                let _bf = match file.read(&mut page_id_buf){
                    Ok(_bf) => _bf,
                    Err(error) => {
                        return Err(CrustyError::CrustyError(format!(
                            "Cannot open or create heap file: {} {} {:?}",
                            file_path.to_string_lossy(),
                            error.to_string(),
                            error
                        )))
                    }
                };
                let page_id = PageId::from_le_bytes(page_id_buf.try_into().unwrap());
                old_hashmap.insert(page_id, index_now as usize);
            }

            Ok(HeapFile {
                //TODO milestone hs
                num_of_pages: Arc::new(RwLock::new(page_num as u16)), 
                file_name: Arc::new(RwLock::new(file_path)),
                page_id_index_hm: Arc::new(RwLock::new(old_hashmap)),
                file_ptr:file,
                container_id,
                read_count: AtomicU16::new(0),
                write_count: AtomicU16::new(0),
            })
        }

    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        //panic!("TODO milestone hs");
        return *self.num_of_pages.read().unwrap();
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    /// Note: that std::io::{Seek, SeekFrom} require Write locks on the underlying std::fs::File
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        //panic!("TODO milestone hs");
        //check //get(&pid).unwrap();
        let index_to_seek = match self.page_id_index_hm.read().unwrap().get(&pid)
        {
            Some(index) => *index,
            None => {
                return Err(CrustyError::CrustyError(format!(
                    "Invalid ID: {}",pid)))}
        };
       
        let mut file = &self.file_ptr;
        //I want to read from a certain index with a given size (4096)
        //https://doc.rust-lang.org/std/os/unix/fs/trait.FileExt.html#tymethod.read_at
        let mut buf:[u8;4096] = [0; 4096];

        // We now read 8 bytes from the offset 10.
        file.seek(SeekFrom::Start(index_to_seek as u64))?;
        let _f = match file.read(&mut buf) {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {:?}",
                    error.to_string(),
                    error
                )))
            }
        };

        let page_result = Page::from_bytes(&buf);

        Ok(page_result)

    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        trace!(
            "Writing page {} to file {}",
            page.get_page_id(),
            self.container_id
        );
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        //panic!("TODO milestone hs");
        //prepare the bytes for insertion
        let byte_to_file:&[u8] = &page.to_bytes();
        //get the page_id to add to hashmap
        let page_id = page.header.0;
        
        //insert the bytes into the file and lemme know the index of where it's stored
        let file_path = self.file_name.read().unwrap();
        //try open the file and if it's not there, create it. No need for buf write,
        //because it's just one write 
        let mut file = &self.file_ptr;

        //To determine where I want to write it, I can use the #of pages*4096
        //and then add at there. 
        let write_index = *self.num_of_pages.read().unwrap() as i32 * 4096;
        //set the cursor there and write 
        file.seek(SeekFrom::Start(write_index as u64))?;
        let _f = match file.write_all(byte_to_file) {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };
        //I must be able to write to it. 
        self.page_id_index_hm.write().unwrap().insert(page_id, write_index as usize);

        //add the num of pages
        *self.num_of_pages.write().unwrap()+=1;
        //if things work out, return ok
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_test_sm_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.to_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
        let new_hf = HeapFile::new(f.to_path_buf(), 0);
        println!("{:?}", *new_hf.unwrap().page_id_index_hm.read().unwrap());
    }
}
