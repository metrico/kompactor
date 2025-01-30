<!-- # ![image-removebg-preview (14)](https://github.com/user-attachments/assets/fd2af745-0ab4-4960-b7df-589189ef8ca1) -->
# <img src="https://github.com/user-attachments/assets/14cf638a-c5e6-468f-bce7-871d1488d355" width=500 />

Kompactor is a DuckDB powered Parquet + Metadata data compactor for InfluxDB3 Core or _"FDAP"_ stack

<img src="https://github.com/user-attachments/assets/1945e6ec-11f3-48ce-b92e-e06481eb4421" width=500 />

> [!WARNING]
> ⚠️ _Experimental, Untested & Unstable - keep backups!_

## Overview

Kompactor extends the lifetime and performance of InfluxDB3 Core by
- Reading InfluxDB 3 snapshot metadata from JSON files
- Compacting and time sorting multiple parquet files daily/hourly
- Updating metadata to reflect the new compacted file structure
- Maintaining correct min/max time ranges and statistics for readers
  

## Prerequisites

- InfluxDB 3 Core w/ File storage
- [Bun](https://bun.sh/) runtime environment
- DuckDB node API package


## Usage

Basic usage pattern:
```bash
bun run kompactor.ts <data-dir> --hosts <host1,host2,...> [options]
```

### Arguments
```bash
Arguments:
    data-dir     Root data directory (e.g., /data)

Options:
    --hosts      Comma-separated list of host folders to process (e.g., my_host,other_host)
    --window     Time window in hours for splitting files (default: 24)
    --dry-run    Run without making any changes
    --verbose    Enable detailed logging
    --help       Show this help message

Example:
    bun run kompactor.ts /data --hosts my_host --dry-run --verbose
    bun run kompactor.ts /data --hosts my_host,other_host --window 12

```

### Examples
```bash
# Dry run with verbose output
bun kompactor.ts ./data --hosts my_host --dry-run --verbose

# Actual compaction
bun kompactor.ts ./data --hosts my_host ./compacted
```

## Features

- Merges multiple parquet files while maintaining time-based sorting
- Preserves metadata structure and relationships
- Calculates and updates aggregate statistics
- Supports dry-run mode for validation
- Detailed logging in verbose mode
- Uses DuckDB for efficient parquet file operations
- Automatic cleanup of DuckDB resources

## Input Format

The tool expects snapshot metadata files with the format:
```json
{
  "writer_id": "host_name",
  "parquet_size_bytes": 398790,
  "row_count": 6854,
  "min_time": 1737928861362000000,
  "max_time": 1737930192543000000,
  "databases": [
    [
      0,
      {
        "tables": [
          [
            3,
            [
              {
                "id": 14,
                "path": "host/dbs/db-0/table-3/2025-01-26/22-00/file.parquet",
                "size_bytes": 10377,
                "row_count": 50,
                "chunk_time": 1737928800000000000,
                "min_time": 1737928874762000000,
                "max_time": 1737929170992000000
              }
            ]
          ]
        ]
      }
    ]
  ]
}
```

## Development

The project is written in TypeScript and uses:
- Bun runtime for modern JavaScript/TypeScript execution
- DuckDB for parquet file operations
- Node.js fs/promises API for file system operations

## Disclaimer

Motivation and background information from this [blog post](https://www.influxdata.com/blog/influxdb3-open-source-public-alpha-jan-27/?utm_source=linkedin&utm_medium=social&utm_campaign=2025-01-13-InfluxDB-3-Launch)


> Bun™, DuckDB™, InfluxDB™ and any other trademarks, service marks, trade names, and product names referenced in this documentation are the property of their respective owners. The use of any trademark, trade name, or product name is for descriptive purposes only and does not imply any affiliation with or endorsement by the trademark owner. All product names, logos, brands, trademarks, and registered trademarks mentioned herein are the property of their respective owners. They are used in this documentation for identification purposes only. Use of these names, logos, trademarks, and brands does not imply endorsement, sponsorship, or affiliation. This project is independent and not affiliated with, endorsed by, or sponsored by any of the companies whose products or technologies are mentioned in this documentation.
