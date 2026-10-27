// SPDX-License-Identifier: AGPL-3.0-only
//! Image detection and thumbnails. The type comes from the bytes, never from the
//! uploader's claim, and decoding is bounded so a small file can't expand into gigabytes.

use std::io::Cursor;

use image::{ImageFormat, ImageReader, Limits};

/// The longest side of a thumbnail, in pixels.
pub const THUMBNAIL_SIZE: u32 = 400;
/// Decoding gives up beyond these (a 25 MB file can claim any size in its header).
const MAX_SIDE: u32 = 12_000;
const MAX_DECODE_BYTES: u64 = 192 * 1024 * 1024;

pub struct Image {
    /// One of [`crate::INLINE_TYPES`].
    pub content_type: &'static str,
    pub width: u32,
    pub height: u32,
    /// JPEG (or PNG when the image has transparency), and its type.
    pub thumbnail: Option<(Vec<u8>, &'static str)>,
}

/// The image in `bytes`, if it is one we serve inline. Undecodable images (truncated,
/// too large) still count by their signature, without size or thumbnail.
pub fn inspect(bytes: &[u8]) -> Option<Image> {
    let format = image::guess_format(bytes).ok()?;
    let content_type = match format {
        ImageFormat::Png => "image/png",
        ImageFormat::Jpeg => "image/jpeg",
        ImageFormat::Gif => "image/gif",
        ImageFormat::WebP => "image/webp",
        _ => return None,
    };
    let decoded = decode(bytes, format);
    let Some(image) = decoded else {
        return Some(Image {
            content_type,
            width: 0,
            height: 0,
            thumbnail: None,
        });
    };
    let (width, height) = (image.width(), image.height());
    // Shrink only: a small image is its own thumbnail.
    let small = if width > THUMBNAIL_SIZE || height > THUMBNAIL_SIZE {
        image.thumbnail(THUMBNAIL_SIZE, THUMBNAIL_SIZE)
    } else {
        image
    };
    let mut out = Cursor::new(Vec::new());
    let thumbnail = if small.color().has_alpha() {
        small
            .write_to(&mut out, ImageFormat::Png)
            .ok()
            .map(|_| "image/png")
    } else {
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 80)
            .encode_image(&small.to_rgb8())
            .ok()
            .map(|_| "image/jpeg")
    };
    Some(Image {
        content_type,
        width,
        height,
        thumbnail: thumbnail.map(|t| (out.into_inner(), t)),
    })
}

/// Decodes within the limits, or None.
fn decode(bytes: &[u8], format: ImageFormat) -> Option<image::DynamicImage> {
    let mut reader = ImageReader::with_format(Cursor::new(bytes), format);
    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_SIDE);
    limits.max_image_height = Some(MAX_SIDE);
    limits.max_alloc = Some(MAX_DECODE_BYTES);
    reader.limits(limits);
    reader.decode().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{DynamicImage, RgbImage, RgbaImage};

    fn encode(image: DynamicImage, format: ImageFormat) -> Vec<u8> {
        let mut out = Cursor::new(Vec::new());
        image.write_to(&mut out, format).unwrap();
        out.into_inner()
    }

    #[test]
    fn photos_get_a_small_jpeg() {
        let png = encode(
            DynamicImage::ImageRgb8(RgbImage::new(1600, 900)),
            ImageFormat::Png,
        );
        let image = inspect(&png).unwrap();
        assert_eq!(
            (image.content_type, image.width, image.height),
            ("image/png", 1600, 900)
        );
        let (thumb, kind) = image.thumbnail.unwrap();
        assert_eq!(kind, "image/jpeg");
        let t = image::load_from_memory(&thumb).unwrap();
        assert_eq!((t.width(), t.height()), (400, 225));
    }

    #[test]
    fn transparency_keeps_png() {
        let png = encode(
            DynamicImage::ImageRgba8(RgbaImage::new(50, 80)),
            ImageFormat::Png,
        );
        let image = inspect(&png).unwrap();
        // Smaller than a thumbnail: kept at its size.
        let (thumb, kind) = image.thumbnail.unwrap();
        assert_eq!(kind, "image/png");
        let t = image::load_from_memory(&thumb).unwrap();
        assert_eq!((t.width(), t.height()), (50, 80));
    }

    #[test]
    fn non_images_and_lies_are_not_images() {
        assert!(inspect(b"%PDF-1.7 ...").is_none());
        assert!(inspect(b"<svg xmlns='http://www.w3.org/2000/svg'><script/></svg>").is_none());
        assert!(inspect(b"<html><script>alert(1)</script>").is_none());
    }

    #[test]
    fn truncated_images_count_by_signature_without_a_thumbnail() {
        let png = encode(
            DynamicImage::ImageRgb8(RgbImage::new(64, 64)),
            ImageFormat::Png,
        );
        let image = inspect(&png[..40]).unwrap();
        assert_eq!(image.content_type, "image/png");
        assert!(image.thumbnail.is_none());
    }

    #[test]
    fn huge_claimed_dimensions_are_refused_before_allocating() {
        // A PNG header claiming 100,000 x 100,000 pixels.
        let mut png = encode(
            DynamicImage::ImageRgb8(RgbImage::new(1, 1)),
            ImageFormat::Png,
        );
        png[16..20].copy_from_slice(&100_000u32.to_be_bytes());
        png[20..24].copy_from_slice(&100_000u32.to_be_bytes());
        let image = inspect(&png).unwrap();
        assert!(image.thumbnail.is_none());
    }
}
