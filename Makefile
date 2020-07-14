default: test

.common.makefile:
	curl -fsSL -o $@ https://gitlab.com/bsm/misc/raw/master/make/go/common.makefile

include .common.makefile


proto: internal/testdata/testdata.pb.go

%.pb.go: %.proto
	protoc -I=. --gogo_out=paths=source_relative:. $<
