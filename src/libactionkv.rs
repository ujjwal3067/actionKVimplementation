#![allow(dead_code, unused_imports)]

use std::fs::OpenOptions; 
use std::fs::File; 
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::io::{self,*};
use serde::{Serialize, Deserialize};

type ByteString = Vec<u8>;

type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair { 
    pub key : ByteString, 
    pub value : ByteString, 
}

#[derivce(Debug)]
pub struct ActionKV { 
    f : File, 
    pub index : HashMap<ByteString, u64>,  // mapping from keys to file location 
}

impl ActionKV { 
    pub fn open(path : &Path) -> io::Result<Self> { 
        let f = OpenOptions::new() 
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new() ; 
        Ok(ActionKV { f , index })
    }
    pub fn load(&mut self) -> io::Result<()> { 
        let mut f = BufReader::new(&mut self.f)  ; 
        loop { 
            let position = f.seek ( SeekFrom ::Current(0))?; 
            let maybe_kv = ActionKV::process_record(&mut f); 
            let kv = match maybe_kv { 
                Ok(kv) => kv, 
                Err(err) => { 
                    match err.kind() { 
                        io::ErrorKind::UnexpectedEof => { 
                            break ; 
                        }
                        _ => return Err(err), 
                    }
                }
            };
            self.index.insert(kv.key, position);
        }
        Ok(())
    }
    pub fn process_record(&mut self, buffer : &mut BufReader<File>) -> std::result::Result<_, Err>{ 
    }  
}

