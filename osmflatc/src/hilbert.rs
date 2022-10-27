use core::mem::size_of;
use core::slice::from_raw_parts_mut;
use core::ptr::copy_nonoverlapping;
use osmflat::*;
use std::time::Instant;

use log::info;
use memmap2::MmapMut;
use rayon::prelude::*;
use std::io::{Error, ErrorKind};
use std::{fs::OpenOptions, path::PathBuf};

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

    let mut mmap = open_mmap(&dir, "hilbert_node_pairs")?;
    let slc = &mut mmap[8..];
    let hilbert_node_pairs =
        unsafe { from_raw_parts_mut(slc.as_ptr() as *mut HilbertNodePair, hilbert_node_pairs_len) };

    info!("Sorting hilbert node pairs...");
    let t = Instant::now();
    hilbert_node_pairs.par_sort_unstable_by_key(|idx| idx.h());
    info!("Finished in {} secs.", t.elapsed().as_secs());

    // Get the original nodes vector to read from.
    let nodes_len = archive.nodes().len();
    let nodes_mmap = open_mmap(&dir, "nodes")?;
    let nodes = unsafe { from_raw_parts_mut(nodes_mmap[8..].as_ptr() as *mut Node, nodes_len) };

    // Setup new nodes vector to place sorted nodes into.
    let mut sorted_nodes_mmap = create_mmap(&dir, "sorted_nodes", 8 + size_of::<Node>() * nodes_len)?;
    
    // Copy the header from the original nodes vector into the sorted nodes vector.
    unsafe {
        copy_nonoverlapping(nodes_mmap[..8].as_ptr(), sorted_nodes_mmap[..8].as_mut_ptr(), 8)
    }

    // let sorted_nodes =
    //     unsafe { from_raw_parts_mut(sorted_nodes_mmap[8..].as_ptr() as *mut Node, nodes_len) };
    let sorted_nodes_ptr = sorted_nodes_mmap[8..].as_ptr() as *mut Node;

    // Parallel iteration through the original nodes vector.
    info!("Copying nodes to sorted nodes vector.");
    let t = Instant::now();
    hilbert_node_pairs
        .iter()
        .enumerate()
        .for_each(move |(i, pair)| {
            let orig_node = &nodes[i as usize];
            unsafe {
                // sorted_nodes.get_unchecked_mut(pair.i() as usize).fill_from(orig_node);
                (*sorted_nodes_ptr.add(pair.i() as usize)).fill_from(orig_node);
            };
        });
    info!("Finished in {} secs.", t.elapsed().as_secs());

    Ok(())
}

fn open_mmap(dir: &PathBuf, file_name: &str) -> std::io::Result<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join(file_name))?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };
    Ok(mmap)
}

fn create_mmap(dir: &PathBuf, file_name: &str, size: usize) -> std::io::Result<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.join(file_name))?;
    file.set_len(size as u64);
    let mmap = unsafe { MmapMut::map_mut(&file)? };
    Ok(mmap)
}
