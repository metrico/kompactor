import { readdir, mkdir, readFile, writeFile, access } from 'node:fs/promises';
import { join, dirname, resolve, sep, basename } from 'node:path';
import duckdb, { DuckDBInstance, Connection } from '@duckdb/node-api';

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

    getHostDir(host) {
        return join(this.dataDir, host);
    }

    getSnapshotsDir(host) {
        return join(this.getHostDir(host), 'snapshots');
    }

    getDbsDir(host) {
        return join(this.getHostDir(host), 'dbs');
    }

    isCompactedFile(path) {
        return basename(path).startsWith('c_');
    }

    getTimeBasedGroups(files) {
        // Sort files by min_time
        const sortedFiles = [...files].sort((a, b) => Number(BigInt(a.min_time) - BigInt(b.min_time)));
        const groups = [];
        let currentGroup = [];
        let currentMinTime = null;

        for (const file of sortedFiles) {
            // Skip already compacted files
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
            const hostDir = this.getHostDir(host);
            try {
                await access(hostDir);
            } catch {
                throw new Error(`Host directory does not exist: ${hostDir}`);
            }

            try {
                await access(this.getSnapshotsDir(host));
            } catch {
                throw new Error(`Snapshots directory does not exist: ${this.getSnapshotsDir(host)}`);
            }

            try {
                await access(this.getDbsDir(host));
            } catch {
                throw new Error(`DBs directory does not exist: ${this.getDbsDir(host)}`);
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

    async compactParquetFiles(host, tableFiles, outputPath) {
        if (!this.connection) throw new Error('DuckDB connection not initialized');
        
        const filePaths = tableFiles.map(f => join(this.getHostDir(host), f.path));
        
        this.log('Compacting files:', filePaths);
        this.log('Output path:', outputPath);
        
        if (this.dryRun) {
            this.log('[DRY-RUN] Would execute DuckDB query to compact files');
            return;
        }

        const fileListQuery = filePaths.map(path => `'${path}'`).join(', ');
        await this.connection.run(`
            COPY (SELECT * FROM read_parquet_mergetree([${fileListQuery}], 'time') TO '${outputPath}' (FORMAT 'parquet');
        `);
    }

    async getParquetStats(filePath, sourceFiles) {
        if (!this.connection) throw new Error('DuckDB connection not initialized');

        if (this.dryRun && sourceFiles) {
            this.log('[DRY-RUN] Calculating aggregate stats from source files');
            
            const aggregateStats = {
                row_count: sourceFiles.reduce((sum, file) => sum + file.row_count, 0),
                size_bytes: sourceFiles.reduce((sum, file) => sum + file.size_bytes, 0),
                min_time: Math.min(...sourceFiles.map(file => file.min_time)),
                max_time: Math.max(...sourceFiles.map(file => file.max_time))
            };
            
            this.log('[DRY-RUN] Aggregate stats:', aggregateStats);
            return aggregateStats;
        }

        const result = await this.connection.run(`
            SELECT 
                COUNT(*) as row_count,
                MIN(min_time) as min_time,
                MAX(max_time) as max_time
            FROM read_parquet('${filePath}')
        `);
        
        const chunks = await result.fetchAllChunks();
        const stats = chunks[0].toArray()[0];
        stats.size_bytes = Bun.file(filePath).size;
        
        this.log('Got stats for compacted file:', stats);
        return stats;
    }

    async processTableGroup(host, tableId, files) {
        this.log(`\nProcessing table group ${tableId} with ${files.length} files`);
        this.log('Files to process:', JSON.stringify(files, null, 2));

        const timeGroups = this.getTimeBasedGroups(files);
        this.log(`Split into ${timeGroups.length} time-based groups`);

        const results = [];
        for (const [groupIndex, groupFiles] of timeGroups.entries()) {
            if (groupFiles.length === 0) continue;

            const firstFile = groupFiles[0];
            const firstFilePath = firstFile.path;
            
            const outputPath = join(
                this.getDbsDir(host),
                ...firstFilePath.split('/').slice(2, -1),
                `c_${firstFile.id}_g${groupIndex}.parquet`
            );
            
            if (this.dryRun) {
                this.log(`[DRY-RUN] Would create directory: ${dirname(outputPath)}`);
                this.log(`[DRY-RUN] Would compact files:`, groupFiles.map(f => basename(f.path)));
            } else {
                await mkdir(dirname(outputPath), { recursive: true });
            }

            await this.compactParquetFiles(host, groupFiles, outputPath);
            const stats = await this.getParquetStats(outputPath, groupFiles);

            const relativePath = join(
                ...firstFilePath.split('/').slice(0, -1),
                basename(outputPath)
            );

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
        const content = await readFile(join(this.getSnapshotsDir(host), filename), 'utf-8');
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
                        dbInfo.tables.set(tId, compactedFiles);
                    }

                    const aggregateStats = {
                        size_bytes: compactedFiles.reduce((sum, f) => sum + f.size_bytes, 0),
                        row_count: compactedFiles.reduce((sum, f) => sum + f.row_count, 0),
                        min_time: Math.min(...compactedFiles.map(f => f.min_time)),
                        max_time: Math.max(...compactedFiles.map(f => f.max_time))
                    };

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
                const snapshotFiles = await readdir(this.getSnapshotsDir(host));
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

                    const outputMetadataPath = join(this.getSnapshotsDir(host), snapshotFile);
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
