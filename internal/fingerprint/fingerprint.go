package fingerprint

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

const anchorSize int64 = 64 * 1024

type Fingerprint struct {
	Size       int64  `json:"size"`
	SampleSize int64  `json:"sample_size"`
	First      string `json:"first"`
	Middle     string `json:"middle"`
	Last       string `json:"last"`
}

func ReaderAt(r io.ReaderAt, size int64, sampleSize int64) (Fingerprint, error) {
	if size < 0 {
		return Fingerprint{}, fmt.Errorf("negative size")
	}
	if sampleSize <= 0 {
		sampleSize = anchorSize
	}
	n := min64(size, anchorSize)
	first, err := hashAt(r, 0, n)
	if err != nil {
		return Fingerprint{}, err
	}
	middleOff := int64(0)
	if size > n {
		middleOff = (size - n) / 2
	}
	middle, err := hashAt(r, middleOff, n)
	if err != nil {
		return Fingerprint{}, err
	}
	lastOff := int64(0)
	if size > n {
		lastOff = size - n
	}
	last, err := hashAt(r, lastOff, n)
	if err != nil {
		return Fingerprint{}, err
	}
	return Fingerprint{Size: size, SampleSize: sampleSize, First: first, Middle: middle, Last: last}, nil
}

func (f Fingerprint) Compatible(other Fingerprint) bool {
	return f.Size == other.Size && f.First != "" && f.First == other.First && f.Middle == other.Middle && f.Last == other.Last
}

func PrefixCompatible(existing, source io.ReaderAt, size int64) (bool, error) {
	existingFP, err := ReaderAt(existing, size, anchorSize)
	if err != nil {
		return false, err
	}
	sourceFP, err := ReaderAt(source, size, anchorSize)
	if err != nil {
		return false, err
	}
	return existingFP.Compatible(sourceFP), nil
}

func hashAt(r io.ReaderAt, off, n int64) (string, error) {
	h := sha256.New()
	if n == 0 {
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	sr := io.NewSectionReader(r, off, n)
	if _, err := io.Copy(h, sr); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
