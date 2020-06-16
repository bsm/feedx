default: vet test

test:
	go test ./...

vet:
	go vet ./...

proto: internal/testdata/testdata.pb.go

%.pb.go: %.proto
	protoc -I=. --gogo_out=paths=source_relative:. $<
