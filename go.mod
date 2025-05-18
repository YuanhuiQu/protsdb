module github.com/yuanhuiqu/protsdb

go 1.21

require (
	github.com/golang/protobuf v1.5.3
	github.com/golang/snappy v0.0.4
	github.com/prometheus/prometheus v0.48.1
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/grafana/regexp v0.0.0-20221122212121-6b5c0a4cb7fd // indirect
	github.com/prometheus/common v0.44.0 // indirect
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

// Prometheus has many dependencies, we need to specify some replacements
replace github.com/prometheus/prometheus => github.com/prometheus/prometheus v0.47.2
