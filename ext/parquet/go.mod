module github.com/bsm/feedx/ext/parquet

go 1.15

require (
	github.com/bsm/feedx v0.12.5
	github.com/bsm/ginkgo v1.16.1
	github.com/bsm/gomega v1.11.0
	github.com/fraugster/parquet-go v0.0.0-00010101000000-000000000000
	go.uber.org/multierr v1.7.0
)

replace github.com/fraugster/parquet-go => github.com/bsm/goparquet v0.3.1-0.20210831105850-a7219b0cd190
