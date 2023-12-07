package src

import "runtime"

// Config glbal config
type Config struct {
	OriginKeys        int
	HotKey            float64
	HotKeyRate        float64
	path              string
	ZipfianConstant   float64
	BlockSize         int
	instanceNumber    int
	parallelingNumber int
}

var config = Config{OriginKeys: 10000, HotKey: 0.2, HotKeyRate: 1, path: "leveldb", ZipfianConstant: 0.8, BlockSize: 200, instanceNumber: 4, parallelingNumber: runtime.NumCPU()}
