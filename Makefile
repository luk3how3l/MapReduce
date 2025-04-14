# Name of binaries
BINARIES = main worker

# Default target: build all
all: $(BINARIES)

# Build main
main: main.go
	go build -o main main.go

# Build worker
worker: worker.go
	go build -o worker worker.go

# Clean up binaries
clean:
	rm -f $(BINARIES)
