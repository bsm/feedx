module github.com/bsm/feedx/ext/parquet

go 1.14

require (
	github.com/bsm/feedx v0.12.2
	github.com/golang/snappy v0.0.1 // indirect
	github.com/kostya-sh/parquet-go v0.0.0-20180827163605-06b7130dc45c
	github.com/onsi/ginkgo v1.13.0
	github.com/onsi/gomega v1.10.1
)

replace github.com/bsm/feedx => ../../
