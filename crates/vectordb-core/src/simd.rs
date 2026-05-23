//! SIMD-accelerated distance primitives (f32x4 via `wide`).

use wide::f32x4;

#[inline]
pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    let n = a.len();
    let chunks = n / 4;
    let mut sum = f32x4::ZERO;
    for i in 0..chunks {
        let base = i * 4;
        let av = f32x4::new([a[base], a[base + 1], a[base + 2], a[base + 3]]);
        let bv = f32x4::new([b[base], b[base + 1], b[base + 2], b[base + 3]]);
        sum += av * bv;
    }
    let mut scalar = sum.reduce_add();
    for i in (chunks * 4)..n {
        scalar += a[i] * b[i];
    }
    scalar
}

#[inline]
pub fn l2_squared_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    let n = a.len();
    let chunks = n / 4;
    let mut sum = f32x4::ZERO;
    for i in 0..chunks {
        let base = i * 4;
        let av = f32x4::new([a[base], a[base + 1], a[base + 2], a[base + 3]]);
        let bv = f32x4::new([b[base], b[base + 1], b[base + 2], b[base + 3]]);
        let d = av - bv;
        sum += d * d;
    }
    let mut scalar = sum.reduce_add();
    for i in (chunks * 4)..n {
        let d = a[i] - b[i];
        scalar += d * d;
    }
    scalar
}
