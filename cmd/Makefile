compile-ebgp-processor:
	CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -a -ldflags '-extldflags "-static"' -o ../bin/ebgp-processor ./ebgp-processor.go
