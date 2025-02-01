import { readdir, mkdir, readFile, writeFile, access } from 'node:fs/promises';
import { join, dirname, resolve, sep, basename } from 'node:path';
import duckdb, { DuckDBInstance } from '@duckdb/node-api';

class ParquetCompactor {
    constructor(dataDir, hosts, options = {}) {
        this.dataDir = dataDir;
        this.hosts = hosts;
        this.dryRun = options.dryRun || false;
        this.verbose = options.verbose || false;
        
        // Configuration matching Rust implementation
        this.maxDesiredFileSizeBytes = 100 * 1024 * 1024;  // 100MB default
        this.percentageMaxFileSize = 30;  // 30% threshold
        this.splitPercentage = 70;        // 70-30 split
        this.timeWindowNanos = BigInt((options.timeWindowHours || 24) * 3600 * 1000000000);
        
        this.instance = null;
        this.connection = null;
        
        this.log = this.verbose ? 
            (...args) => console.log('[INFO]', ...args) : 
            () => {};
    }

    computeCutoffBytes() {
        const small = (this.maxDesiredFileSizeBytes * this.percentageMaxFileSize) / 100;
        const large = (this.maxDesiredFileSizeBytes * (100 + this.percentageMaxFileSize)) / 100;
        return [small, large];
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

    isCompactedFile(path) {
        return basename(path).startsWith('c_');
    }

    extractWalSequence(path) {
        const match = path.match(/(\d{10})\.parquet$/);
        if (!match) {
            throw new Error(`Could not extract WAL sequence number from path: ${path}`);
        }
        return match[1];
    }

    getTimeBasedGroups(files) {
        // Don't split if total size is small enough
        const totalSize = files.reduce((sum, f) => sum + f.size_bytes, 0);
        const [smallCutoff, largeCutoff] = this.computeCutoffBytes();

        if (totalSize <= smallCutoff) {
            return [files];
        }

        const sortedFiles = [...files].sort((a, b) => Number(BigInt(a.min_time) - BigInt(b.min_time)));
        const groups = [];
        let currentGroup = [];
        let currentSize = 0;
        let currentMinTime = null;

        for (const file of sortedFiles) {
            if (this.isCompactedFile(file.path)) {
                this.log(`Skipping already compacted file: ${file.path}`);
                continue;
            }

            if (currentGroup.length === 0) {
                currentGroup = [file];
                currentMinTime = BigInt(file.min_time);
                currentSize = file.size_bytes;
                continue;
            }

            const fileMinTime = BigInt(file.min_time);
            const timeGap = fileMinTime - currentMinTime;
            const wouldExceedSize = currentSize + file.size_bytes > this.maxDesiredFileSizeBytes;

            if (wouldExceedSize || timeGap > this.timeWindowNanos) {
                if (currentGroup.length > 0) {
                    groups.push(currentGroup);
                }
                currentGroup = [file];
                currentMinTime = fileMinTime;
                currentSize = file.size_bytes;
            } else {
                currentGroup.push(file);
                currentSize += file.size_bytes;
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
        const mergeQuery = `COPY (SELECT * FROM read_parquet([${fileListQuery}]) ORDER BY time) TO '${outputPath}' (
            FORMAT 'parquet',
            COMPRESSION 'ZSTD',
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

            // Create unique name for compacted file using WAL sequence
            const walSeq = this.extractWalSequence(firstFile.path);
            const compactedFileName = `c_${walSeq}_${firstFile.id}_g${groupIndex}.parquet`;
            
            const outputPathParts = [...pathParts.slice(0, -1), compactedFileName];
            const relativePath = outputPathParts.join('/');
            const outputPath = join(this.dataDir, relativePath);
            
            if (this.dryRun) {
                this.log(`[DRY-RUN] Would create directory: ${dirname(outputPath)}`);
                this.log(`[DRY-RUN] Would merge and delete files:`, groupFiles.map(f => basename(f.path)));
                this.log(`[DRY-RUN] Output path: ${relativePath}`);
            } else {
                await mkdir(dirname(outputPath), { recursive: true });
                await this.mergeParquetFiles(host, groupFiles, outputPath);                
                // Delete original files only after successful merge
                for (const file of groupFiles) {
                    const filePath = join(this.dataDir, file.path);
                    this.log(`Deleting original file: ${filePath}`);
                    await Bun.file(filePath).delete();

                }
            }

            const stats = {
                row_count: groupFiles.reduce((sum, f) => sum + f.row_count, 0),
                size_bytes: groupFiles.reduce((sum, f) => sum + f.size_bytes, 0),
                min_time: Math.min(...groupFiles.map(f => f.min_time)),
                max_time: Math.max(...groupFiles.map(f => f.max_time)),
                chunk_time: groupFiles[0].chunk_time
            };

            results.push({
                id: firstFile.id,
                path: relativePath,
                size_bytes: stats.size_bytes,
                row_count: stats.row_count,
                chunk_time: stats.chunk_time,
                min_time: stats.min_time,
                max_time: stats.max_time
            });
        }

        return results;
    }

    async updateMetadata(metadata, tableId, compactedFiles) {
        this.log('\nUpdating metadata for table:', tableId);
        if (compactedFiles.length === 0) {
            this.log('No compacted files to update in metadata');
            return metadata;
        }

        this.log('Compacted files info:', JSON.stringify(compactedFiles, null, 2));

        for (const [dbId, dbInfo] of metadata.databases) {
            for (const [tId, files] of dbInfo.tables) {
                if (tId === tableId) {
                    this.log(`Found matching table ${tId} in database ${dbId}`);
                    this.log('Current files:', JSON.stringify(files, null, 2));
                    
                    if (this.dryRun) {
                        this.log('[DRY-RUN] Would replace files with:', JSON.stringify(compactedFiles, null, 2));
                    } else {
                        // Find and update the specific table entry
                        const tableEntry = dbInfo.tables.find(([id]) => id === tId);
                        if (tableEntry) {
                            tableEntry[1] = compactedFiles;
                        }
                    }

                    // Maintain global metadata statistics
                    const allFiles = [...files, ...compactedFiles];
                    const stats = {
                        row_count: allFiles.reduce((sum, f) => sum + f.row_count, 0),
                        size_bytes: allFiles.reduce((sum, f) => sum + f.size_bytes, 0),
                        min_time: Math.min(...allFiles.map(f => f.min_time)),
                        max_time: Math.max(...allFiles.map(f => f.max_time))
                    };
                    
                    if (!this.dryRun) {
                        metadata.parquet_size_bytes = stats.size_bytes;
                        metadata.row_count = stats.row_count;
                        metadata.min_time = stats.min_time;
                        metadata.max_time = stats.max_time;
                    }
                    break;
                }
            }
        }

        return metadata;
    }

    async readSnapshotMetadata(host, filename) {
        const content = await readFile(join(this.dataDir, host, 'snapshots', filename), 'utf-8');
        return JSON.parse(content);
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
                            this.log(`Processing table ${tableId} with ${files.length} files...`);
                            
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
    --help       Show this help message

Example:
    bun run kompactor.ts /data --hosts my_host --dry-run --verbose
    bun run kompactor.ts /data --hosts my_host,other_host
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

console.log(`Starting compactor with:
Data directory: ${dataDir}
Hosts to process: ${hosts.join(', ')}
Dry run: ${dryRun}
Verbose: ${verbose}
`);

const compactor = new ParquetCompactor(dataDir, hosts, { 
    dryRun, 
    verbose 
});

try {
    await compactor.compact();
    console.log('Compaction completed successfully!');
} catch (error) {
    console.error('Error during compaction:', error);
    process.exit(1);
}
