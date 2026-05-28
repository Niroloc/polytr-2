// Package util holds small math helpers used by the FP models.
package util

import "math"

// NormCDF is the standard normal CDF via the Abramowitz & Stegun 7.1.26
// approximation (max error ~1.5e-7). Cheap enough to call on every tick.
func NormCDF(x float64) float64 {
	// Use complementary erfc relation: Φ(x) = 0.5 * erfc(-x / √2)
	return 0.5 * math.Erfc(-x/math.Sqrt2)
}

// NormPDF is the standard normal PDF.
func NormPDF(x float64) float64 {
	return math.Exp(-0.5*x*x) / math.Sqrt(2*math.Pi)
}

// Clamp restricts x to [lo, hi].
func Clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

// MeanStd computes population mean and stddev of xs. Returns (0,0) if empty.
func MeanStd(xs []float64) (float64, float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range xs {
		sum += v
	}
	mean := sum / float64(len(xs))
	var sq float64
	for _, v := range xs {
		d := v - mean
		sq += d * d
	}
	sd := math.Sqrt(sq / float64(len(xs)))
	return mean, sd
}
