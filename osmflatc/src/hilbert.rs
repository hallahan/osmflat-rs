use core::mem::size_of;
use core::ptr::copy_nonoverlapping;
use core::slice::{from_raw_parts, from_raw_parts_mut};
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

    // -- Initialize tags_index and sorted_tags_index

    // Setup new tags_index vector to place list of tags as we reorder the entities.
    // The sorted index will be built up as we work through the OSM entities.
    let tags_index = archive.tags_index();
    let tags_index_len = tags_index.len();
    let tags_index_mmap: MmapMut = open_mmap(&dir, "tags_index")?;
    let mut sorted_tags_index_mmap = create_mmap(
        &dir,
        "sorted_tags_index",
        8 + size_of::<TagIndex>() * tags_index_len,
    )?;
    // Copy the header from the original nodes vector into the sorted nodes vector.
    unsafe {
        copy_nonoverlapping(
            tags_index_mmap[..8].as_ptr(),
            sorted_tags_index_mmap[..8].as_mut_ptr(),
            8,
        )
    }
    // Cast buffer to slice of TagIndex
    let sorted_tags_index: &mut [TagIndex] = unsafe {
        from_raw_parts_mut(
            sorted_tags_index_mmap[8..].as_ptr() as *mut TagIndex,
            tags_index_len,
        )
    };
    let mut tag_counter: usize = 0;

    // -- Reorder nodes.

    // Get the original nodes vector to read from.
    let nodes: &[Node] = archive.nodes();
    let nodes_len = nodes.len();
    let nodes_mmap: MmapMut = open_mmap(&dir, "nodes")?;

    // Setup new nodes vector to place sorted nodes into.
    let mut sorted_nodes_mmap =
        create_mmap(&dir, "sorted_nodes", 8 + size_of::<Node>() * nodes_len)?;

    // Copy the header from the original nodes vector into the sorted nodes vector.
    unsafe {
        copy_nonoverlapping(
            nodes_mmap[..8].as_ptr(),
            sorted_nodes_mmap[..8].as_mut_ptr(),
            8,
        )
    }
    // Cast buffer to slice of Nodes.
    let sorted_nodes: &mut [Node] =
        unsafe { from_raw_parts_mut(sorted_nodes_mmap[8..].as_ptr() as *mut Node, nodes_len) };

    info!("Reordering nodes.");
    let t = Instant::now();
    sorted_nodes
        .iter_mut()
        .zip(hilbert_node_pairs.iter_mut())
        .for_each(|(sorted_node, hilbert_node_pair)| {
            let i = hilbert_node_pair.i() as usize;
            let node = &nodes[i];
            
            let start = node.tag_first_idx() as usize;
            let end = node.tags().end as usize;
            
            for t in &tags_index[start..end] {
                sorted_tags_index[tag_counter].fill_from(t);
                tag_counter += 1;
            }

            sorted_node.fill_from(node);
            sorted_node.set_tag_first_idx(start as u64);
        });
    info!("Finished in {} secs.", t.elapsed().as_secs());

    // 


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
    file.set_len(size as u64)?;
    let mmap = unsafe { MmapMut::map_mut(&file)? };
    Ok(mmap)
}
