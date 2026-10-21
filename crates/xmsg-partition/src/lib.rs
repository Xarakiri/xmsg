// SPDX-License-Identifier: AGPL-3.0-only
//! Kafka-compatible partitioning of channel IDs.
//!
//! `partition(channel_id, n)` returns exactly what Kafka's default partitioner
//! returns for a key serialized with `LongSerializer`. See `spec/partitioning`.

const SEED: u32 = 0x9747_b28c;
const M: u32 = 0x5bd1_e995;
const R: u32 = 24;

/// Kafka's `Utils.murmur2`, returning the same signed 32-bit value as the Java code.
pub fn murmur2(data: &[u8]) -> i32 {
    let len = data.len();
    let mut h: u32 = SEED ^ (len as u32);

    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let rest = chunks.remainder();
    if rest.len() == 3 {
        h ^= (rest[2] as u32) << 16;
    }
    if rest.len() >= 2 {
        h ^= (rest[1] as u32) << 8;
    }
    if !rest.is_empty() {
        h ^= rest[0] as u32;
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// The Kafka key for a channel: its ID as 8-byte big-endian two's complement.
pub fn channel_key(channel_id: i64) -> [u8; 8] {
    channel_id.to_be_bytes()
}

/// Partition of `channel_id` in a topic with `partition_count` partitions.
///
/// # Panics
/// If `partition_count` is zero.
pub fn partition(channel_id: i64, partition_count: u32) -> u32 {
    assert!(partition_count > 0, "partition_count must be positive");
    let positive = (murmur2(&channel_key(channel_id)) as u32) & 0x7fff_ffff;
    positive % partition_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_kafka_values() {
        // From spec/partitioning/vectors.csv.
        assert_eq!(murmur2(&channel_key(0)), -522_759_933);
        assert_eq!(partition(0, 512), 259);
        assert_eq!(partition(1, 16), 12);
    }

    #[test]
    #[should_panic(expected = "partition_count must be positive")]
    fn zero_partitions_panics() {
        partition(1, 0);
    }
}
