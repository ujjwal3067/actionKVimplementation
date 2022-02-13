#![allow(dead_code, unused_imports)]

use std::fs::OpenOptions; 
use std::fs::File; 
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::io::{self,*};
use byteorder::LittleEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use crc::crc32;
use serde::{Serialize, Deserialize};

type ByteString = Vec<u8>;

type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair { 
    pub key : ByteString, 
    pub value : ByteString, 
}

#[derive(Debug)]
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

    fn process_record<R : Read> (f : &mut R) -> io::Result<KeyValuePair>
    { 
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let val_len = f.read_u32::<LittleEndian>()?;
        let data_len = key_len + val_len ; 
        let mut data = ByteString::with_capacity(data_len as usize);
        { 
            f.by_ref() 
                .take(data_len as u64)
                .read_to_end(&mut data)?;
        }

        debug_assert_eq!(data.len() , data_len as usize);
        let checksum = crc32::checksum_ieee(&data);
        //NOTE: checking if the checksum produced is what we expect it to be
        if checksum != saved_checksum { 
            panic!( 
                "data corruption encountered ({:08x} != {:08x})", 
                checksum, saved_checksum 

            )
        }

        let value = data.split_off(key_len as usize);
        let key = data ; 
        Ok(KeyValuePair { key, value })
    } 

    pub fn insert(
        &mut self, 
        key : &ByteStr, 
        value : &ByteStr
        )-> io::Result<()>{ 
        let position = self.insert_but_ignore_index(key, value)?; 
        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    pub fn insert_but_ignore_index(
        &mut self, 
        key : &ByteStr, 
        value : &ByteStr, 
        ) -> io::Result<u64> { 
        let mut f = BufWriter::new(&mut self.f); 
        let key_len = key.len() ; 
        let val_len = value.len() ; 
        let mut tmp = ByteString::with_capacity(key_len + val_len); 
        for byte in key { 
            tmp.push(*byte);
        }
        for byte in value { 
            tmp.push(*byte);
        }

        let checksum = crc32::checksum_ieee(&tmp);
        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?; 
        f.write_all(&mut tmp)?;
        Ok(current_position)
    }
} 
