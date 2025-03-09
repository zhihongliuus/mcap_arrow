# MCAP Arrow Project

## Introduction

The MCAP Arrow project is a C++ library that integrates the [MCAP file format](https://mcap.dev/) with the [Apache Arrow](https://arrow.apache.org/) data processing framework. This library enables efficient serialization, deserialization, and processing of Arrow data structures within MCAP files, providing a powerful solution for time-series data management with columnar data analytics capabilities.

## Features

- **Arrow Integration**: Convert between MCAP messages and Arrow tables/record batches
- **Efficient Indexing**: Fast lookups and range-based queries for Arrow data in MCAP files
- **Streaming Processing**: Read and write Arrow data in a streaming fashion
- **IPC Support**: Inter-Process Communication for sharing Arrow data between processes
- **Compression Support**: Configurable compression options for Arrow data
- **Time-based Navigation**: Query and extract Arrow data by timestamp ranges

## Architecture

The library consists of several core components:

### Core Components

- **McapArrowConverter**: Converts between MCAP messages and Arrow tables
- **McapArrowWriter**: Writes Arrow tables to MCAP files
- **McapArrowReader**: Reads Arrow tables from MCAP files 
- **McapArrowIndex**: Provides indexing capabilities for efficient data access
- **McapArrowIPC**: Enables Inter-Process Communication with Arrow data

### IPC System

The IPC module provides functionality for:

- Shared memory communication between processes
- Synchronization primitives (mutex, condition variables)
- Atomic counter operations
- Producer-consumer pattern for Arrow data transfer

## Dependencies

This project uses a hybrid approach for dependency management:

### Bzlmod Dependencies (MODULE.bazel)

Standard dependencies with registry support:
- **rules_cc**: C++ rules for Bazel
- **protobuf**: Protocol Buffers library
- **abseil-cpp**: Abseil C++ library
- **googletest**: Google's C++ testing framework
- **rules_proto**: Protocol Buffer rules for Bazel

### Local Dependencies (WORKSPACE)

Custom local dependencies:
- **apache_arrow**: Apache Arrow C++ library
- **mcap**: MCAP file format library
- **lz4**: LZ4 compression library
- **zstd**: Zstandard compression library
- **boost**: Boost C++ libraries

## Building and Testing

### Prerequisites

- Bazel 6.0 or newer
- C++17 compatible compiler
- The required dependencies installed in the `third_party` directory

### Build Commands

Build the library:

```bash
bazel build //src/lib:mcap_arrow
```

Run the core tests (no Arrow dependencies):

```bash
bazel test //src/test:core_tests --check_direct_dependencies=off --enable_workspace
```

Run all tests (including Arrow-dependent tests):

```bash
bazel test //src/test:all_tests --check_direct_dependencies=off --enable_workspace
```

### Test Suite Organization

The test suite is organized into two main groups:

1. **Core Tests**: Basic functionality tests that don't require external dependencies
   - Simple tests
   - Minimal MCAP Arrow tests
   - Basic IPC tests

2. **Arrow Tests**: Tests that require the Apache Arrow library
   - Converter tests
   - Writer tests
   - Reader tests
   - Index tests
   - IPC tests

## Usage Examples

### Converting MCAP to Arrow

```cpp
#include "src/lib/mcap_arrow_converter.h"
#include "src/lib/mcap_arrow_reader.h"

// Create a reader for an MCAP file
mcap_arrow::McapArrowReader reader;
reader.open("data.mcap");

// Read the MCAP file as an Arrow table
auto table = reader.readAsTable();

// Process the Arrow table
// ...

// Close the reader
reader.close();
```

### Writing Arrow Data to MCAP

```cpp
#include "src/lib/mcap_arrow_writer.h"

// Create a writer for an MCAP file
mcap_arrow::McapArrowWriter writer;
writer.open("output.mcap");

// Create an Arrow table
// ...

// Write the Arrow table to the MCAP file
writer.writeTable("/my_topic", table, "timestamp");

// Close the writer
writer.close();
```

### Using the IPC Functionality

```cpp
#include "src/lib/mcap_arrow_ipc.h"

// Writer process
mcap_arrow::McapArrowIpcWriter writer;
writer.initialize("my_shared_memory", 1024*1024);

// Write an Arrow table to shared memory
writer.writeTable("/my_topic", table, "timestamp");

// Reader process
mcap_arrow::McapArrowIpcReader reader;
reader.initializeFromSharedMemory("my_shared_memory");

// Wait for new data
if (reader.waitForData(1000)) {
    // Process the data
    // ...
}
```

## IPC Functionality

The Inter-Process Communication (IPC) functionality allows sharing Arrow data between processes efficiently using shared memory:

### Key IPC Components

- **SharedMemoryInfo**: Contains information about the shared memory segment
- **IpcSyncContext**: Provides synchronization primitives for IPC
- **McapArrowIpcWriter**: Writes Arrow data to shared memory
- **McapArrowIpcReader**: Reads Arrow data from shared memory

### Synchronization Features

- Mutex-based locking for concurrent access
- Condition variables for event notification
- Atomic counters for tracking operations
- Timeout-based waiting for new data

## Dependency Management

This project uses a hybrid approach for dependency management:

1. **Bzlmod (MODULE.bazel)**: Used for standard dependencies with registry support:
   - rules_cc
   - protobuf
   - abseil-cpp
   - googletest
   - rules_proto

2. **WORKSPACE**: Used for local dependencies that require custom build files:
   - apache_arrow
   - mcap
   - lz4
   - zstd
   - boost

To build and test the project:

```bash
# Run core tests
bazel test //src/test:core_tests --check_direct_dependencies=off --enable_workspace

# Run all tests including Arrow-dependent tests
bazel test //src/test:all_tests --check_direct_dependencies=off --enable_workspace
```

Note: In the future, as Bzlmod matures and more dependencies become available through the registry, we plan to fully migrate away from the WORKSPACE file.

## Future Plans

1. **Full Bzlmod Migration**: As Bzlmod matures, we plan to migrate away from WORKSPACE for dependency management
2. **Extended Arrow Integration**: Support for more Arrow data types and operations
3. **Performance Optimizations**: Improvements for large dataset handling
4. **Cloud Storage Support**: Direct reading/writing to cloud storage platforms
5. **Streaming Queries**: Implement streaming query capabilities on Arrow data

## Contributing

Contributions to the MCAP Arrow project are welcome. Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Write or update tests as necessary
5. Ensure all tests pass
6. Submit a pull request

Please follow the existing code style and add appropriate documentation for new features.

## References

- [MCAP GitHub Repository](https://github.com/foxglove/mcap)
- [MCAP Documentation](https://mcap.dev/)
- [Apache Arrow](https://arrow.apache.org/)
- [Bazel Build System](https://bazel.build/)
