//! Scalar quantization (8-bit per dimension) for memory-efficient storage.

use serde::{Deserialize, Serialize};

/// Per-dimension min/max tracked online for scalar quantization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarQuantizer {
    pub dimension: usize,
    pub min: Vec<f32>,
    pub max: Vec<f32>,
    pub trained: bool,
}

impl ScalarQuantizer {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            min: vec![f32::INFINITY; dimension],
            max: vec![f32::NEG_INFINITY; dimension],
            trained: false,
        }
    }

    pub fn observe(&mut self, vector: &[f32]) {
        if vector.len() != self.dimension {
            return;
        }
        for (i, &v) in vector.iter().enumerate() {
            if v < self.min[i] {
                self.min[i] = v;
            }
            if v > self.max[i] {
                self.max[i] = v;
            }
        }
        self.trained = true;
    }

    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut out = vec![0u8; self.dimension];
        for (i, &v) in vector.iter().enumerate() {
            let lo = self.min.get(i).copied().unwrap_or(0.0);
            let hi = self.max.get(i).copied().unwrap_or(1.0);
            let span = (hi - lo).max(1e-8);
            let t = ((v - lo) / span).clamp(0.0, 1.0);
            out[i] = (t * 255.0).round() as u8;
        }
        out
    }

    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        let mut out = vec![0.0f32; self.dimension];
        for (i, &code) in codes.iter().enumerate() {
            let lo = self.min.get(i).copied().unwrap_or(0.0);
            let hi = self.max.get(i).copied().unwrap_or(1.0);
            let span = hi - lo;
            out[i] = lo + (code as f32 / 255.0) * span;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_approximate() {
        let mut q = ScalarQuantizer::new(3);
        let v = [0.0, 0.5, 1.0];
        q.observe(&v);
        let codes = q.encode(&v);
        let back = q.decode(&codes);
        for (a, b) in v.iter().zip(back.iter()) {
            assert!((a - b).abs() < 0.02);
        }
    }
}
