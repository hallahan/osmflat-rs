use core::mem::size_of;
use core::ptr::copy_nonoverlapping;
use core::slice::from_raw_parts_mut;
use std::sync::atomic::AtomicU64;
use fast_hilbert::xy2h;
use geo::algorithm::interior_point::InteriorPoint;
use geo::geometry::{Coordinate, LineString};
use geo::{coord, Polygon};
use log::info;
use memmap2::MmapMut;
use osmflat::*;
use pbr::ProgressBar;
use rayon::prelude::*;
use std::io::{Error, ErrorKind};
use std::time::Instant;
use std::{fs::OpenOptions, path::PathBuf};

pub fn process(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let archive: Osm = Osm::open(FileResourceStorage::new(dir.clone()))?;

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

    let ways_len = archive.ways().len();

    let hilbert_way_pairs_mmap = create_mmap(
        dir,
        "hilbert_way_pairs",
        8 + size_of::<HilbertWayPair>() * ways_len,
    )?;

    // What is usually put in those first 8 bytes?
    // unsafe {
    //     copy_nonoverlapping(mmap[..8].as_ptr(), mmap[..8].as_mut_ptr(), 8);
    // }

    // Cast buffer to slice of HilbertWayPairs.
    let hilbert_way_pairs: &mut [HilbertWayPair] = unsafe {
        from_raw_parts_mut(
            hilbert_way_pairs_mmap[8..].as_ptr() as *mut HilbertWayPair,
            ways_len,
        )
    };

    // We already know the hilbert location for nodes.
    // We could actually do that work here and decouple it from osmflatc completely,
    // but an extra loop over the nodes especially will take longer.
    // We need to know the hilbert location for ways and relations.
    build_hilbert_way_pairs(hilbert_way_pairs, &archive)?;


    let hilbert_node_pairs_mmap = open_mmap(&dir, "hilbert_node_pairs")?;
    let hilbert_node_pairs = unsafe {
        from_raw_parts_mut(
            hilbert_node_pairs_mmap[8..].as_ptr() as *mut HilbertNodePair,
            hilbert_node_pairs_len,
        )
    };

    info!("Sorting hilbert node pairs.");
    let t = Instant::now();
    hilbert_node_pairs.par_sort_unstable_by_key(|idx| idx.h());
    info!("Finished in {} secs.", t.elapsed().as_secs());

    info!("Sorting hilbert way pairs.");
    let t = Instant::now();
    hilbert_way_pairs.par_sort_unstable_by_key(|idx| idx.h());
    info!("Finished in {} secs.", t.elapsed().as_secs());

    // -- Initialize tags_index and sorted_tags_index --

    // Setup new tags_index vector to place list of tags as we reorder the entities.
    // The sorted index will be built up as we work through the OSM entities.
    let tags_index = archive.tags_index();
    let nodes_index = archive.nodes_index();
    let tags_index_len = tags_index.len();
    let nodes_index_len = nodes_index.len();
    let tags_index_mmap: MmapMut = open_mmap(&dir, "tags_index")?;
    let nodes_index_mmap: MmapMut = open_mmap(&dir, "nodes_index")?;

    let mut sorted_tags_index_mmap = create_mmap(
        &dir,
        "sorted_tags_index",
        8 + size_of::<TagIndex>() * tags_index_len,
    )?;

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

    let mut sorted_nodes_index_mmap = create_mmap(
        &dir,
        "sorted_nodes_index",
        8 + size_of::<NodeIndex>() * nodes_index_len,
    )?;

    unsafe {
        copy_nonoverlapping(
            nodes_index_mmap[..8].as_ptr(),
            sorted_nodes_index_mmap[..8].as_mut_ptr(),
            8,
        )
    }
    let sorted_nodes_index: &mut [NodeIndex] = unsafe {
        from_raw_parts_mut(
            sorted_nodes_index_mmap[8..].as_ptr() as *mut NodeIndex,
            nodes_index_len,
        )
    };

    let mut tag_counter: usize = 0;
    let mut nodes_index_counter: usize = 0;

    // -- Reorder nodes. --

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

    let mut prog_counter: usize = 0;
    let prog_len = (nodes_len / 1_000_000) as u64;
    let mut pb = ProgressBar::new(prog_len as u64);
    pb.message("Reordering nodes.");
    let t = Instant::now();

    sorted_nodes
        .iter_mut()
        .zip(hilbert_node_pairs.iter_mut())
        .for_each(|(sorted_node, hilbert_node_pair)| {
            let i = hilbert_node_pair.i() as usize;
            let node = &nodes[i];
            let start = node.tag_first_idx() as usize;
            let end = node.tags().end as usize;

            let tag_first_idx = tag_counter;
            for t in &tags_index[start..end] {
                sorted_tags_index[tag_counter].fill_from(t);
                tag_counter += 1;
            }

            sorted_node.fill_from(node);
            sorted_node.set_tag_first_idx(tag_first_idx as u64);
            if i / 1_000_000 > prog_counter {
                pb.inc();
                prog_counter += 1;
            }
        });
    info!("Finished in {} secs.", t.elapsed().as_secs());

    // -- Reorder ways. --

    // Get the original ways vector to read from.
    let ways: &[Way] = archive.ways();
    let ways_len = ways.len();
    let ways_mmap: MmapMut = open_mmap(&dir, "ways")?;

    // Setup new ways vector to place sorted ways into.
    let mut sorted_ways_mmap = create_mmap(&dir, "sorted_ways", 8 + size_of::<Way>() * ways_len)?;

    // Copy the header from the original ways vector into the sorted ways vector.
    unsafe {
        copy_nonoverlapping(
            ways_mmap[..8].as_ptr(),
            sorted_ways_mmap[..8].as_mut_ptr(),
            8,
        )
    }
    // Cast buffer to slice of Ways.
    let sorted_ways: &mut [Way] =
        unsafe { from_raw_parts_mut(sorted_ways_mmap[8..].as_ptr() as *mut Way, ways_len) };
    
    let mut prog_counter: usize = 0;
    let prog_len = (ways_len / 1_000_000) as u64;
    let mut pb = ProgressBar::new(prog_len as u64);
    pb.message("Reordering ways.");
    let t = Instant::now();  
    
    sorted_ways
        .iter_mut()
        .zip(hilbert_way_pairs.iter_mut())
        .for_each(|(sorted_way, hilbert_way_pair)| {
            let i = hilbert_way_pair.i() as usize;
            let way = &ways[i];
            let start = way.tag_first_idx() as usize;
            let end = way.tags().end as usize;

            let tag_first_idx = tag_counter;
            for t in &tags_index[start..end] {
                sorted_tags_index[tag_counter].fill_from(t);
                tag_counter += 1;
            }

            let ref_start = way.ref_first_idx() as usize;
            let ref_end = way.refs().end as usize;

            let nodes_first_idx = nodes_index_counter;
            for r in &nodes_index[ref_start..ref_end] {
                sorted_nodes_index[nodes_index_counter].fill_from(r);
                nodes_index_counter += 1;
            }

            sorted_way.fill_from(way);
            sorted_way.set_tag_first_idx(tag_first_idx as u64);
            sorted_way.set_ref_first_idx(nodes_first_idx as u64);

            if i / 1_000_000 > prog_counter {
                pb.inc();
                prog_counter += 1;
            }
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
    file.set_len(size as u64)?;
    let mmap = unsafe { MmapMut::map_mut(&file)? };
    Ok(mmap)
}

pub fn build_hilbert_way_pairs(
    hilbert_way_pairs: &mut [HilbertWayPair],
    archive: &Osm,
) -> Result<(), Box<dyn std::error::Error>> {
    let nodes = archive.nodes();
    let nodes_index = archive.nodes_index();
    let ways = archive.ways();

    info!("Building hilbert way pairs.");
    let t = Instant::now();

    hilbert_way_pairs
        .par_iter_mut()
        .enumerate()
        .for_each(|(i, pair)| {
            let way = &archive.ways()[i];

            // Calculate point on surface.
            // http://libgeos.org/doxygen/classgeos_1_1algorithm_1_1InteriorPointArea.html
            // https://docs.rs/geo/latest/geo/algorithm/interior_point/trait.InteriorPoint.html
            // https://github.com/georust/geo/blob/main/geo/src/algorithm/interior_point.rs

            let refs = way.refs();
            let len = refs.end - refs.start;
            let mut coords = Vec::<Coordinate<f64>>::with_capacity(len as usize);

            for r in refs {
                if let Some(idx) = nodes_index[r as usize].value() {
                    let node = &nodes[idx as usize];
                    let lon = node.lon() as f64;
                    let lat = node.lat() as f64;
                    coords.push(coord! { x: lon, y: lat });
                };
            }

            let location = if coords.first() == coords.last() {
                Polygon::new(LineString::new(coords), vec![]).interior_point()
            } else {
                LineString::new(coords).interior_point()
            };

            if let Some(loc) = location {
                let x = (loc.x() as i64 + i32::MAX as i64) as u32;
                let y = (loc.x() as i64 + i32::MAX as i64) as u32;
                // info!("way point on surface {:#?}", loc);
                let h = xy2h(x, y);

                pair.set_i(i as u64);
                pair.set_h(h);
            } else {
                eprintln!(
                "Unable to find point on surface to compute hilbert location for way at index {}.",
                i
            );
            }
        });

    info!("Finished in {} secs.", t.elapsed().as_secs());
    Ok(())
}
