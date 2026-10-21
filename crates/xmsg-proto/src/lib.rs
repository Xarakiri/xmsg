// SPDX-License-Identifier: AGPL-3.0-only
//! Rust types for the xmsg Protobuf contracts (`proto/xmsg/v1`).

#[allow(clippy::all, missing_docs)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/xmsg.v1.rs"));
}

pub use prost::Message;

#[cfg(test)]
mod tests {
    use super::v1::{ClientFrame, Connect, client_frame::Frame};
    use prost::Message;

    #[test]
    fn client_frame_round_trips() {
        let frame = ClientFrame {
            frame: Some(Frame::Connect(Connect {
                access_token: "dev:1".into(),
                protocol_version: 1,
                ..Default::default()
            })),
        };
        let decoded = ClientFrame::decode(frame.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, frame);
    }
}
