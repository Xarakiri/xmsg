// SPDX-License-Identifier: AGPL-3.0-only
//! Checks every row of spec/partitioning/vectors.csv.

use xmsg_partition::{channel_key, murmur2, partition};

const VECTORS: &str = include_str!("../../../spec/partitioning/vectors.csv");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn matches_shared_vectors() {
    let mut lines = VECTORS.lines();
    let header: Vec<&str> = lines.next().expect("header").split(',').collect();
    let counts: Vec<u32> = header[3..]
        .iter()
        .map(|c| c.trim_start_matches('p').parse().expect("partition count"))
        .collect();

    let mut rows = 0;
    for line in lines.filter(|l| !l.is_empty()) {
        let cols: Vec<&str> = line.split(',').collect();
        let channel_id: i64 = cols[0].parse().expect("channel_id");
        let key = channel_key(channel_id);
        assert_eq!(hex(&key), cols[1], "key bytes for {channel_id}");
        assert_eq!(
            murmur2(&key),
            cols[2].parse::<i32>().unwrap(),
            "murmur2 for {channel_id}"
        );
        for (i, n) in counts.iter().enumerate() {
            let expected: u32 = cols[3 + i].parse().unwrap();
            assert_eq!(
                partition(channel_id, *n),
                expected,
                "partition({channel_id}, {n})"
            );
        }
        rows += 1;
    }
    assert!(rows > 200, "expected the full vector file, got {rows} rows");
}
