default: test

.common.makefile:
	curl -fsSL -o $@ https://gitlab.com/bsm/misc/raw/master/make/go/common.makefile

include .common.makefile

proto: internal/testdata/testdata.pb.go

%.pb.go: %.proto
	# may need to `go install google.golang.org/protobuf/cmd/protoc-gen-go`
	protoc -I=. --go_out=paths=source_relative:. $<
