use osmflat::*;
use std::time::{Duration, Instant};

use log::info;
use memmap2::MmapMut;
use rayon::prelude::*;
use std::io::{Error, ErrorKind};
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

pub fn process(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let archive = Osm::open(FileResourceStorage::new(dir.clone()))?;

    let hilbert_node_pairs_len = archive
        .hilbert_node_pairs()
        .unwrap_or(vec![].as_slice())
        .len();

    if hilbert_node_pairs_len == 0 {
        return Err(Box::new(Error::new(
            ErrorKind::NotFound,
            "No hilbert node pairs!",
        )));
    }

    let nodes_len = archive.nodes().len();
    let ways_len = archive.ways().len();
    let relations_len = archive.relations().len();

    let mut hilbert_node_pairs_buf = open_mmap(&dir, "hilbert_node_pairs")?;
    let slc = &mut hilbert_node_pairs_buf[8..];
    let hilbert_node_pairs = unsafe {
        let pairs = ::core::slice::from_raw_parts_mut(
            slc.as_ptr() as *mut HilbertNodePair,
            hilbert_node_pairs_len,
        );
        pairs
    };

    // for idx in &hilbert_node_pairs[..30] {
    //     println!("hilbert_node_pairs  i {} h {}", idx.i(), idx.h());
    // }

    info!("Sorting hilbert node pairs...");
    let t = Instant::now();
    hilbert_node_pairs.par_sort_unstable_by_key(|idx| idx.h());
    info!(
        "Finished sorting hilbert node pairs in {} secs.",
        t.elapsed().as_secs()
    );

    Ok(())
}

fn open_mmap(dir: &PathBuf, file_name: &str) -> std::io::Result<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.join(file_name))?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };
    Ok(mmap)
}
