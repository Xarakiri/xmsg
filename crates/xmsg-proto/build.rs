// SPDX-License-Identifier: AGPL-3.0-only
//! Compiles proto/xmsg/v1/*.proto with prost. Needs `protoc` on PATH (or PROTOC).

use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let dir = root.join("xmsg/v1");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "proto"))
        .collect();
    files.sort();
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    prost_build::compile_protos(&files, &[root])
}
