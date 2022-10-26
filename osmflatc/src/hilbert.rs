use logging_timer::{timer, finish};
use osmflat::*;

use std::{fs::OpenOptions, path::PathBuf};
use memmap2::MmapMut;
use std::io::{Error, ErrorKind};
use log::info;
use rayon::prelude::*;

pub fn process(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let archive = Osm::open(FileResourceStorage::new(dir.clone()))?;
    
    let hilbert_node_pairs_len = archive.hilbert_node_pairs().unwrap_or(vec![].as_slice()).len();

    if hilbert_node_pairs_len == 0 {
        return Err(Box::new(Error::new(ErrorKind::NotFound, "No hilbert node pairs!")));
    }

    let nodes_len = archive.nodes().len();
    let ways_len = archive.ways().len();
    let relations_len = archive.relations().len();

    let hilbert_node_pairs_buf = open_mmap(&dir, "hilbert_node_pairs")?; 
    let hilbert_node_pairs = unsafe {
        convert_buf_to::<HilbertNodePair>(hilbert_node_pairs_buf, hilbert_node_pairs_len)
    };

    info!("Sorting hilbert node pairs...");
    let t = timer!("Sort hilbert node pairs.");
    hilbert_node_pairs.par_sort_unstable_by_key(|idx| idx.h());
    finish!(t, "Finished sorting hilbert node pairs");
    

    Ok(())
}

fn open_mmap(dir: & PathBuf, file_name: &str) -> std::io::Result<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.join(file_name))?;

    let mmap = unsafe {
        MmapMut::map_mut(&file)?
    };
    Ok(mmap)
}

unsafe fn convert_buf_to<T>(mut buf: MmapMut, len: usize) -> &'static mut [T] {
    // Ignore first 8 bytes (header)
    let slc = &mut buf[8..];
    let t_slc = ::core::slice::from_raw_parts_mut( slc.as_ptr() as *mut T, len);
    t_slc
}
