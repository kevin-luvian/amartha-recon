.PHONY: test

test:
	echo "Running tests and generating coverage report..."
	go test -coverprofile=coverage.out `go list ./... | grep -v ./cmd`

run:
	echo "Running main cmd"
	go run cmd/main.go
