# protsdb

A time series database implementation inspired by Prometheus TSDB, written in Go.

## Overview

ProtSDB (Prometheus Time Series Database) is a Go implementation that replicates core functionalities of Prometheus's time series database. 

Currently in early development. 


### Data Flow
1. Remote write requests are received via HTTP
2. Data is written to WAL for durability
3. Samples are stored in the head block
4. Periodically, head block data is compacted into persistent blocks
5. (TBD)Queries merge results from both head and persistent blocks

