import { readdir, mkdir, readFile, writeFile, access } from 'node:fs/promises';
import { join, dirname, resolve, sep, basename } from 'node:path';
import duckdb, { DuckDBInstance } from '@duckdb/node-api';

class ParquetCompactor {
    constructor(dataDir, hosts, options = {}) {
        this.dataDir = dataDir;
        this.hosts = hosts;
        this.dryRun = options.dryRun || false;
        this.verbose = options.verbose || false;
        this.timeWindowNanos = BigInt((options.timeWindowHours || 24) * 3600 * 1000000000);
        this.instance = null;
        this.connection = null;
        
        this.log = this.verbose ? 
            (...args) => console.log('[INFO]', ...args) : 
            () => {};
    }

    normalizePath(path) {
        return path.replace(/\/+/g, '/');
    }

    validateSourcePath(path, host) {
        const parts = path.split('/');
        if (parts[0] !== host) {
            throw new Error(`Invalid path: ${path}, must start with host: ${host}`);
        }
        if (parts[1] !== 'dbs') {
            throw new Error(`Invalid path: ${path}, second component must be 'dbs'`);
        }
        
        if (parts.length !== 7) {
            throw new Error(`Invalid path depth: ${path}, expected 7 components, got ${parts.length}`);
        }
        
        if (!parts[6].endsWith('.parquet')) {
            throw new Error(`Invalid file extension: ${path}, must end with .parquet`);
        }

        return true;
    }

    verifyPathCoherence(sourcePath, compactedPath, host) {
        const normalizedSource = this.normalizePath(sourcePath);
        const normalizedCompacted = this.normalizePath(compactedPath);

        const sourceParts = normalizedSource.split('/');
        const compactedParts = normalizedCompacted.split('/');

        if (sourceParts.length !== compactedParts.length) {
            throw new Error(`Path structure mismatch: ${sourcePath} vs ${compactedPath}`);
        }

        for (let i = 0; i < sourceParts.length - 1; i++) {
            if (sourceParts[i] !== compactedParts[i]) {
                throw new Error(`Path component mismatch at position ${i}: ${sourceParts[i]} vs ${compactedParts[i]}`);
            }
        }

        const sourceFile = sourceParts[sourceParts.length - 1];
        const compactedFile = compactedParts[compactedParts.length - 1];
        if (!compactedFile.startsWith('c_') || !compactedFile.endsWith('.parquet')) {
            throw new Error(`Invalid compacted file name convention: ${compactedFile}`);
        }

        return true;
    }

    isCompactedFile(path) {
        return basename(path).startsWith('c_');
    }

    getTimeBasedGroups(files) {
        const sortedFiles = [...files].sort((a, b) => Number(BigInt(a.min_time) - BigInt(b.min_time)));
        const groups = [];
        let currentGroup = [];
        let currentMinTime = null;
    
        for (const file of sortedFiles) {
            if (this.isCompactedFile(file.path)) {
                this.log(`Skipping already compacted file: ${file.path}`);
                continue;
            }
    
            if (!currentGroup.length) {
                currentGroup.push(file);
                currentMinTime = BigInt(file.min_time);
                continue;
            }
    
            const fileMaxTime = BigInt(file.max_time);
            if (fileMaxTime - currentMinTime <= this.timeWindowNanos) {
                currentGroup.push(file);
            } else {
                if (currentGroup.length > 0) {
                    groups.push(currentGroup);
                }
                currentGroup = [file];
                currentMinTime = BigInt(file.min_time);
            }
        }
    
        if (currentGroup.length > 0) {
            groups.push(currentGroup);
        }
    
        return groups;
    }

    async validateDirectories() {
        try {
            await access(this.dataDir);
        } catch {
            throw new Error(`Data directory does not exist: ${this.dataDir}`);
        }

        for (const host of this.hosts) {
            const hostDir = join(this.dataDir, host);
            try {
                await access(hostDir);
            } catch {
                throw new Error(`Host directory does not exist: ${hostDir}`);
            }

            try {
                await access(join(hostDir, 'snapshots'));
            } catch {
                throw new Error(`Snapshots directory does not exist: ${join(hostDir, 'snapshots')}`);
            }

            try {
                await access(join(hostDir, 'dbs'));
            } catch {
                throw new Error(`DBs directory does not exist: ${join(hostDir, 'dbs')}`);
            }
        }
    }

    async initializeDuckDB() {
        this.log('Initializing DuckDB instance...');
        this.log('DuckDB version:', duckdb.version());

        this.instance = await DuckDBInstance.create(':memory:', {
            threads: '4'
        });
        this.connection = await this.instance.connect();

        const initSteps = [
            "INSTALL chsql FROM community;",
            "LOAD chsql;"
        ];

        for (const step of initSteps) {
            this.log(`Executing DuckDB init step: ${step}`);
            if (!this.dryRun) {
                await this.connection.run(step);
            } else {
                this.log(`[DRY-RUN] Would execute: ${step}`);
            }
        }

        this.log('DuckDB extensions initialized successfully');
    }

    getAggregateStats(groupFiles) {
        return {
            row_count: groupFiles.reduce((sum, file) => sum + file.row_count, 0),
            size_bytes: groupFiles.reduce((sum, file) => sum + file.size_bytes, 0),
            min_time: Math.min(...groupFiles.map(file => file.min_time)),
            max_time: Math.max(...groupFiles.map(file => file.max_time))
        };
    }

    computeSplitTime(chunkTimes, minTime, maxTime, totalSize, maxDesiredFileSize) {
        if (totalSize <= maxDesiredFileSize) {
            return [maxTime];
        }
    
        if (minTime === maxTime) {
            return [maxTime];
        }
    
        const splitTimes = [];
        const percentage = maxDesiredFileSize / totalSize;
        const interval = Math.ceil((maxTime - minTime) * percentage);
    
        let min = minTime;
        while (true) {
            const splitTime = min + interval;
            if (splitTime >= maxTime) {
                break;
            } else if (this.timeRangePresent(chunkTimes, min, splitTime)) {
                splitTimes.push(splitTime);
            }
            min = splitTime;
        }
    
        return splitTimes;
    }
    
    timeRangePresent(chunkTimes, minTime, maxTime) {
        return chunkTimes.some(chunk => chunk.max >= minTime && chunk.min <= maxTime);
    }

    async mergeParquetFiles(host, tableFiles, outputPath) {
        if (!this.connection) throw new Error('DuckDB connection not initialized');
        
        const filePaths = tableFiles.map(f => join(this.dataDir, f.path));
        
        this.log('Merging files:', filePaths);
        this.log('Output path:', outputPath);
        
        if (this.dryRun) {
            this.log('[DRY-RUN] Would merge files');
            return;
        }

        const fileListQuery = filePaths.map(path => `'${path}'`).join(', ');
        // const mergeQuery = `COPY (SELECT * FROM read_parquet_mergetree([${fileListQuery}], 'time')) TO '${outputPath}' (FORMAT 'parquet');`;
        const mergeQuery = `COPY (SELECT * FROM read_parquet([${fileListQuery}]) ORDER BY time) TO '${outputPath}' (
            FORMAT 'parquet',
            COMPRESSION 'SNAPPY',
            ROW_GROUP_SIZE 100000
        );`;

        await this.connection.run(mergeQuery);
    }

    async processTableGroup(host, tableId, files) {
        this.log(`\nProcessing table group ${tableId} with ${files.length} files`);
        this.log('Files to process:', JSON.stringify(files, null, 2));

        for (const file of files) {
            this.validateSourcePath(file.path, host);
        }

        const timeGroups = this.getTimeBasedGroups(files);
        this.log(`Split into ${timeGroups.length} time-based groups`);

        const results = [];
        for (const [groupIndex, groupFiles] of timeGroups.entries()) {
            if (groupFiles.length === 0) continue;

            const firstFile = groupFiles[0];
            const firstFilePath = firstFile.path;
            const pathParts = firstFilePath.split('/');
            
            const compactedFileName = `c_${firstFile.id}_g${groupIndex}.parquet`;
            const outputPathParts = [...pathParts.slice(0, -1), compactedFileName];
            const relativePath = outputPathParts.join('/');
            
            this.verifyPathCoherence(firstFilePath, relativePath, host);
            const outputPath = join(this.dataDir, relativePath);
            
            if (this.dryRun) {
                this.log(`[DRY-RUN] Would create directory: ${dirname(outputPath)}`);
                this.log(`[DRY-RUN] Would merge and delete files:`, groupFiles.map(f => basename(f.path)));
                this.log(`[DRY-RUN] Output path: ${relativePath}`);
            } else {
                await mkdir(dirname(outputPath), { recursive: true });
                await this.mergeParquetFiles(host, groupFiles, outputPath);
                // Delete old files after merging
                for (const file of groupFiles) {
                    const filePath = join(this.dataDir, file.path);
                    this.log(`Deleting old file: ${filePath}`);
                    await unlink(filePath);
                }
            }

            const stats = this.getAggregateStats(groupFiles);
            results.push({
                id: firstFile.id,
                path: relativePath,
                size_bytes: stats.size_bytes,
                row_count: stats.row_count,
                chunk_time: firstFile.chunk_time,
                min_time: stats.min_time,
                max_time: stats.max_time
            });
        }

        return results;
    }

    async readSnapshotMetadata(host, filename) {
        const content = await readFile(join(this.dataDir, host, 'snapshots', filename), 'utf-8');
        return JSON.parse(content);
    }

    async updateMetadata(metadata, tableId, compactedFiles) {
    this.log('\nUpdating metadata for table:', tableId);
    this.log('Compacted files info:', JSON.stringify(compactedFiles, null, 2));

    for (const [dbId, dbInfo] of metadata.databases) {
        for (const [tId, files] of dbInfo.tables) {
            if (tId === tableId) {
                this.log(`Found matching table ${tId} in database ${dbId}`);
                this.log('Original files:', JSON.stringify(files, null, 2));
                
                if (this.dryRun) {
                    this.log('[DRY-RUN] Would replace files with:', JSON.stringify(compactedFiles, null, 2));
                } else {
                    const index = dbInfo.tables.findIndex(([id]) => id === tId);
                    dbInfo.tables[index][1] = compactedFiles;
                }

                const aggregateStats = this.getAggregateStats(compactedFiles);
                if (this.dryRun) {
                    this.log('[DRY-RUN] Would update metadata statistics to:', aggregateStats);
                } else {
                    metadata.parquet_size_bytes = aggregateStats.size_bytes;
                    metadata.row_count = aggregateStats.row_count;
                    metadata.min_time = aggregateStats.min_time;
                    metadata.max_time = aggregateStats.max_time;
                }
            }
        }
    }

    return metadata;
}

    async compact() {
        try {
            await this.validateDirectories();
            await this.initializeDuckDB();
            
            for (const host of this.hosts) {
                this.log(`\nProcessing host: ${host}`);
                const snapshotFiles = await readdir(join(this.dataDir, host, 'snapshots'));
                const jsonFiles = snapshotFiles.filter(file => file.endsWith('.info.json'));

                for (const snapshotFile of jsonFiles) {
                    const metadata = await this.readSnapshotMetadata(host, snapshotFile);
                    
                    for (const [dbId, dbInfo] of metadata.databases) {
                        for (const [tableId, files] of dbInfo.tables) {
                            console.log(`Processing table ${tableId} with ${files.length} files...`);
                            
                            const compactedFiles = await this.processTableGroup(host, tableId, files);
                            await this.updateMetadata(metadata, tableId, compactedFiles);
                        }
                    }

                    const outputMetadataPath = join(this.dataDir, host, 'snapshots', snapshotFile);
                    if (!this.dryRun) {
                        await writeFile(
                            outputMetadataPath,
                            JSON.stringify(metadata, null, 2)
                        );
                    } else {
                        this.log(`[DRY-RUN] Would write updated metadata to: ${outputMetadataPath}`);
                    }
                }
            }
        } finally {
            this.connection = null;
            this.instance = null;
        }
    }
}

const usage = `
Usage: bun run kompactor.ts <data-dir> --hosts <host1,host2,...> [options]

Arguments:
    data-dir     Root data directory (e.g., /data)
    --hosts      Comma-separated list of host folders to process (e.g., my_host,other_host)

Options:
    --dry-run    Run without making any changes
    --verbose    Enable detailed logging
    --window     Time window in hours for splitting files (default: 24)
    --help       Show this help message

Example:
    bun run kompactor.ts /data --hosts my_host --dry-run --verbose
    bun run kompactor.ts /data --hosts my_host,other_host --window 12
`;

const args = process.argv.slice(2);

if (args.includes('--help') || args.length < 1) {
    console.log(usage);
    process.exit(args.includes('--help') ? 0 : 1);
}

const dataDir = args[0];
const hostsIndex = args.indexOf('--hosts');
if (hostsIndex === -1 || !args[hostsIndex + 1]) {
    console.error('Error: --hosts parameter is required');
    console.log(usage);
    process.exit(1);
}

const hosts = args[hostsIndex + 1].split(',');
const dryRun = args.includes('--dry-run');
const verbose = args.includes('--verbose');
const windowIndex = args.indexOf('--window');
const timeWindowHours = windowIndex !== -1 ? parseInt(args[windowIndex + 1]) : 24;

console.log(`Starting compactor with:
Data directory: ${dataDir}
Hosts to process: ${hosts.join(', ')}
Time window: ${timeWindowHours}h
Dry run: ${dryRun}
Verbose: ${verbose}
`);

const compactor = new ParquetCompactor(dataDir, hosts, { 
    dryRun, 
    verbose,
    timeWindowHours 
});

try {
    await compactor.compact();
    console.log('Compaction completed successfully!');
} catch (error) {
    console.error('Error during compaction:', error);
    process.exit(1);
}
