// SPDX-License-Identifier: AGPL-3.0-only
//! Delivery streams to gateways: one per (partition, owner_epoch) per gateway,
//! with consecutive sequence numbers (ADR-0009).

use std::collections::HashMap;

use xmsg_proto::v1::{StreamHead, StreamHeads};

#[derive(Default)]
pub struct Streams {
    /// partition → owner_epoch, set when this instance (re)gains the partition.
    epochs: HashMap<u32, u64>,
    /// (gateway, partition) → last sequence sent.
    seqs: HashMap<(String, u32), u64>,
}

impl Streams {
    /// Starts a new stream for `partition` (after an assignment): a new epoch
    /// tells gateways to reset tracking instead of seeing a gap.
    pub fn reset_partition(&mut self, partition: u32, epoch: u64) {
        self.epochs.insert(partition, epoch);
        self.seqs.retain(|(_, p), _| *p != partition);
    }

    /// The (owner_epoch, seq) to stamp on the next batch for this gateway and partition.
    pub fn next(&mut self, gateway: &str, partition: u32, default_epoch: u64) -> (u64, u64) {
        let epoch = *self.epochs.entry(partition).or_insert(default_epoch);
        let seq = self
            .seqs
            .entry((gateway.to_string(), partition))
            .or_insert(0);
        *seq += 1;
        (epoch, *seq)
    }

    /// Heads for one gateway, sent every second so it notices a lost last batch.
    pub fn heads_for(&self, gateway: &str) -> StreamHeads {
        let heads = self
            .seqs
            .iter()
            .filter(|((g, _), _)| g == gateway)
            .map(|((_, p), &seq)| StreamHead {
                partition: *p,
                owner_epoch: self.epochs.get(p).copied().unwrap_or(0),
                seq,
            })
            .collect();
        StreamHeads { heads }
    }

    pub fn gateways(&self) -> Vec<String> {
        let mut g: Vec<String> = self.seqs.keys().map(|(g, _)| g.clone()).collect();
        g.sort();
        g.dedup();
        g
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequences_are_per_gateway_and_partition_and_reset_with_a_new_epoch() {
        let mut s = Streams::default();
        s.reset_partition(3, 100);
        assert_eq!(s.next("gw-a", 3, 0), (100, 1));
        assert_eq!(s.next("gw-a", 3, 0), (100, 2));
        assert_eq!(s.next("gw-b", 3, 0), (100, 1));
        s.reset_partition(3, 200);
        assert_eq!(s.next("gw-a", 3, 0), (200, 1));
        let heads = s.heads_for("gw-a").heads;
        assert_eq!(heads.len(), 1);
        assert_eq!(
            (heads[0].partition, heads[0].owner_epoch, heads[0].seq),
            (3, 200, 1)
        );
    }
}
