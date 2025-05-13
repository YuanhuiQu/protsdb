module protsdb

go 1.21

require (
	github.com/golang/protobuf v1.5.3
	github.com/golang/snappy v0.0.4
	github.com/prometheus/prometheus v0.47.2
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

// Prometheus has many dependencies, we need to specify some replacements
replace github.com/prometheus/prometheus => github.com/prometheus/prometheus v0.47.2
