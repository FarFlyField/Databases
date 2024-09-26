use crate::page::Offset;
use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use serde::de::value;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};

/// The struct for a heap file. 
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    // Track this HeapFile's container Id
    pub container_id: ContainerId,
    pub page_count: Arc<RwLock<PageId>>,
    pub file_path: Arc<RwLock<PathBuf>>,
    pub page_offsets: Arc<RwLock<HashMap<PageId, usize>>>, 
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
         
        let metadata = match File::metadata(&file)
        {
            Ok(data) => data,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot obtain file metadata: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };
        println!("METADATA: {:?}", metadata);
        
        let file_len = metadata.len();
        if file_len == 0
        {
           return Ok(HeapFile {
                container_id,
                page_count: Arc::new(RwLock::new(0)),
                file_path: Arc::new(RwLock::new(file_path)),
                page_offsets: Arc::new(RwLock::new(HashMap::new())),
                read_count: AtomicU16::new(0),
                write_count: AtomicU16::new(0),
            });
        }
        else 
        {   
            let page_count = file_len / 4096;
            let mut page_offsets: HashMap<PageId, usize> = HashMap::new();

            for i in 0..page_count
            {        
                let _seek_pos = match file.seek(SeekFrom::Start((i * 4096) as u64))
                {
                    Ok(value) => value,
                    Err(error) => {
                        return Err(CrustyError::CrustyError(format!(
                            "Seek error in {} {} {:?}",
                            file_path.to_string_lossy(),
                            error.to_string(),
                            error
                    )))}
                };
        
        
                let mut page_bytes = [0; 2];
                let _dummy = match file.read(&mut page_bytes)
                {
                    Ok(value) => value,
                    Err(error) => {
                        return Err(CrustyError::CrustyError(format!(
                            "Cannot read heap file: {} {} {:?}",
                            file_path.to_string_lossy(),
                            error.to_string(),
                            error
                        )))
                    }
                };
                
                let new_page_id = PageId::from_le_bytes(page_bytes.try_into().unwrap());
                page_offsets.insert(new_page_id, (i * 4096) as usize);
            }

            return Ok(HeapFile {
                container_id,
                page_count: Arc::new(RwLock::new(page_count as u16)),
                file_path: Arc::new(RwLock::new(file_path)),
                page_offsets: Arc::new(RwLock::new(page_offsets)),
                read_count: AtomicU16::new(0),
                write_count: AtomicU16::new(0),
            });
        }
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        *(self.page_count.read().unwrap())
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

        let page_offset = match self.page_offsets.read().unwrap().get(&pid) 
        {
            Some(offset) => *offset,
            None => { 
                return Err(CrustyError::CrustyError(format!(
                "Invalid PageId: {}",
                pid
            )))}
        };

        let mut open_file = match OpenOptions::new().read(true).open(&*self.file_path.read().unwrap())
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open heap file: {} {} {:?}",
                    self.file_path.read().unwrap().to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };


        let _seek_pos = match open_file.seek(SeekFrom::Start(page_offset as u64))
        {
            Ok(value) => value,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Seek error in {} {} {:?}",
                    self.file_path.read().unwrap().to_string_lossy(),
                    error.to_string(),
                    error
            )))}
        };


        let mut page_bytes = [0; PAGE_SIZE];
        let _dummy = match open_file.read(&mut page_bytes)
        {
            Ok(value) => value,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot read heap file: {} {} {:?}",
                    self.file_path.read().unwrap().to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };
        
        let page_struct = Page::from_bytes(&page_bytes);
        
        Ok(page_struct)
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

        let mut open_file = match OpenOptions::new()
                                            .read(true)
                                            .write(true)
                                            .open(&*self.file_path.read().unwrap())
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open heap file: {} {} {:?}",
                    self.file_path.read().unwrap().to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

        let new_page_id = page.get_page_id();
        let offset: usize;
        // Account for existing page.
        if self.page_offsets.read().unwrap().contains_key(&new_page_id)
        {
            offset = *self.page_offsets.read().unwrap().get(&new_page_id).unwrap();
        }
        else 
        {
            *self.page_count.write().unwrap() += 1;
            // Append the page.
            offset = match self.page_offsets.read().unwrap().values().max()
            {
                Some(x) => x + PAGE_SIZE,
                None => 0
            };
            
            self.page_offsets.write().unwrap().insert(new_page_id, offset);
        }

        let page_bytes = page.to_bytes();

        let _seek_pos = match open_file.seek(SeekFrom::Start(offset as u64))
        {
            Ok(value) => value,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Seek error in {} {} {:?}",
                    self.file_path.read().unwrap().to_string_lossy(),
                    error.to_string(),
                    error
            )))}
        };

        let _write_res = match open_file.write(&page_bytes)
        {
            Ok(size) => size,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot write heap file: {} {} {:?}",
                    self.file_path.read().unwrap().to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

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

        // println!("{}", hf.num_pages());

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
    }
}
