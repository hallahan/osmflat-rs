#!/bin/zsh
rm -rf /Users/n/geodata/flatdata/california/*
cargo run --release -- /Users/n/geodata/extracts/california.osm.pbf /Users/n/geodata/flatdata/california --ids --hilbert
# cargo run --release -- /Users/n/geodata/extracts/santacruz.pbf /Users/n/geodata/flatdata/santacruz --ids --hilbert