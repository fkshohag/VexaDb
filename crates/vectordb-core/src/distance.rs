use crate::collection::DistanceMetric;

/// Distance / similarity helpers. Lower distance = more similar for L2;
/// higher score = more similar for cosine/dot (returned as negated distance where needed).
pub struct Distance;

impl Distance {
    pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| {
                let d = x - y;
                d * d
            })
            .sum()
    }

    pub fn dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        let dot = Self::dot(a, b);
        let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            return 1.0;
        }
        1.0 - (dot / (norm_a * norm_b))
    }

    /// Compare vectors for HNSW search. Returns a value where **smaller is better**.
    pub fn compare(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
        match metric {
            DistanceMetric::Euclidean => Self::l2_squared(a, b),
            DistanceMetric::Cosine => Self::cosine_distance(a, b),
            DistanceMetric::DotProduct => -Self::dot(a, b),
        }
    }

    /// Convert internal distance to user-facing score (higher = more similar).
    pub fn to_score(metric: DistanceMetric, distance: f32) -> f32 {
        match metric {
            DistanceMetric::Euclidean => -distance,
            DistanceMetric::Cosine => 1.0 - distance,
            DistanceMetric::DotProduct => -distance,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_vectors_have_zero_cosine_distance() {
        let a = [1.0, 0.0, 0.0];
        let b = [1.0, 0.0, 0.0];
        assert!((Distance::cosine_distance(&a, &b) - 0.0).abs() < 1e-6);
    }
}
