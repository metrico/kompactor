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
        // Try original format first
        let match = path.match(/(\d{10})\.parquet$/);
        if (match) return match[1];
        
        // Try compacted format
        match = path.match(/c_(\d{10})_\d+_[gh]\d+\.parquet$/);
        if (match) return match[1];
        
        throw new Error(`Could not extract WAL sequence number from path: ${path}`);
    }

    getTimeBasedGroups(files) {
        const hourlyGroups = new Map();
        
        // First pass: group files by hour
        for (const file of files) {
            const match = file.path.match(/(\d{4}-\d{2}-\d{2})\/(\d{2})/);
            if (!match) {
                this.log(`Warning: Could not extract date/hour from path: ${file.path}`);
                continue;
            }
            
            const [_, date, hour] = match;
            const key = `${date}_${hour}`;
            
            if (!hourlyGroups.has(key)) {
                hourlyGroups.set(key, []);
            }
            hourlyGroups.get(key).push(file);
        }

        const result = [];
        for (const [key, hourFiles] of hourlyGroups) {
            // Only process groups with multiple files
            if (hourFiles.length > 1) {
                this.log(`Found group ${key} with ${hourFiles.length} files`);
                
                // Sort files by WAL sequence
                const sortedFiles = hourFiles.sort((a, b) => {
                    const seqA = parseInt(this.extractWalSequence(a.path));
                    const seqB = parseInt(this.extractWalSequence(b.path));
                    return seqA - seqB;
                });
                
                result.push(sortedFiles);
                this.log(`Added group ${key} for compaction`);
            }
        }

        return result;
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
            await this.connection.run(step);
        }

        this.log('DuckDB extensions initialized successfully');
    }

    async mergeParquetFiles(host, tableFiles, outputPath) {
        if (!this.connection) throw new Error('DuckDB connection not initialized');
        
        const filePaths = tableFiles.map(f => join(this.dataDir, f.path));
        
        this.log('Merging files:', filePaths);
        this.log('Output path:', outputPath);

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

        const timeGroups = this.getTimeBasedGroups(files);
        this.log(`Split into ${timeGroups.length} time-based groups`);

        const results = [];
        for (const groupFiles of timeGroups) {
            if (groupFiles.length === 0) continue;

            const firstFile = groupFiles[0];
            const match = firstFile.path.match(/(\d{4}-\d{2}-\d{2})\/(\d{2})/);
            if (!match) continue;

            const [_, date, hour] = match;
            const basePath = firstFile.path.split('/').slice(0, -2).join('/');
            const outputDir = `${basePath}/${date}/${hour}-00`;
            
            const firstWalSeq = this.extractWalSequence(groupFiles[0].path);
            const lastWalSeq = this.extractWalSequence(groupFiles[groupFiles.length - 1].path);
            const compactedFileName = `c_${firstWalSeq}_${lastWalSeq}_h${hour}.parquet`;
            
            const relativePath = `${outputDir}/${compactedFileName}`;
            const outputPath = join(this.dataDir, relativePath);

            this.log(`Compacting ${groupFiles.length} files to ${relativePath}`);

            await mkdir(dirname(outputPath), { recursive: true });
            await this.mergeParquetFiles(host, groupFiles, outputPath);
            
            // Delete original files and their directories
            for (const file of groupFiles) {
                const filePath = join(this.dataDir, file.path);
                const dirPath = dirname(filePath);
                await Bun.file(filePath).delete();
                try {
                    const remaining = await readdir(dirPath);
                    if (remaining.length === 0) {
                        await Bun.file(dirPath).delete();
                    }
                } catch (err) {
                    this.log(`Warning: Could not remove directory ${dirPath}: ${err}`);
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

    async readSnapshotMetadata(host, filename) {
        const content = await readFile(join(this.dataDir, host, 'snapshots', filename), 'utf-8');
        return JSON.parse(content);
    }

    async updateMetadata(metadata, tableId, compactedFiles) {
        this.log('\nUpdating metadata for table:', tableId);
        if (compactedFiles.length === 0) {
            this.log('No compacted files to update in metadata');
            return metadata;
        }

        this.log('Compacted files:', JSON.stringify(compactedFiles, null, 2));

        for (const [dbId, dbInfo] of metadata.databases) {
            const tableEntry = dbInfo.tables.find(([id]) => id === tableId);
            if (tableEntry) {
                tableEntry[1] = compactedFiles;
                
                const allFiles = [...compactedFiles];
                const stats = {
                    row_count: allFiles.reduce((sum, f) => sum + f.row_count, 0),
                    size_bytes: allFiles.reduce((sum, f) => sum + f.size_bytes, 0),
                    min_time: Math.min(...allFiles.map(f => f.min_time)),
                    max_time: Math.max(...allFiles.map(f => f.max_time))
                };
                
                metadata.parquet_size_bytes = stats.size_bytes;
                metadata.row_count = stats.row_count;
                metadata.min_time = stats.min_time;
                metadata.max_time = stats.max_time;
                break;
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
            
            // Collect all files across all metadata files first
            const allFiles = new Map(); // hour -> files[]
            
            for (const snapshotFile of jsonFiles) {
                this.log('Processing metadata file:', snapshotFile);
                const metadata = await this.readSnapshotMetadata(host, snapshotFile);
                
                // Get files from this metadata
                for (const [dbId, dbInfo] of metadata.databases) {
                    for (const [tableId, files] of dbInfo.tables) {
                        for (const file of files) {
                            const match = file.path.match(/(\d{4}-\d{2}-\d{2})\/(\d{2})/);
                            if (!match) continue;
                            
                            const [_, date, hour] = match;
                            const key = `${date}_${hour}`;
                            
                            if (!allFiles.has(key)) {
                                allFiles.set(key, []);
                            }
                            allFiles.get(key).push(file);
                        }
                    }
                }
            }
            
            // Now process each hour's files
            let changed = false;
            for (const [hourKey, files] of allFiles) {
                if (files.length <= 1) continue;
                
                this.log(`Processing ${files.length} files for hour ${hourKey}`);
                const [date, hour] = hourKey.split('_');
                
                // Sort files by WAL sequence
                const sortedFiles = files.sort((a, b) => {
                    return parseInt(this.extractWalSequence(a.path)) - 
                           parseInt(this.extractWalSequence(b.path));
                });

                // Create compacted file path
                const basePath = sortedFiles[0].path.split('/').slice(0, -2).join('/');
                const firstWalSeq = this.extractWalSequence(sortedFiles[0].path);
                const lastWalSeq = this.extractWalSequence(sortedFiles[sortedFiles.length - 1].path);
                const outputDir = `${basePath}/${date}/${hour}-00`;
                const compactedFileName = `c_${firstWalSeq}_${lastWalSeq}_h${hour}.parquet`;
                const relativePath = `${outputDir}/${compactedFileName}`;
                const outputPath = join(this.dataDir, relativePath);

                this.log(`Compacting to: ${relativePath}`);

                // Create directory and merge files
                await mkdir(dirname(outputPath), { recursive: true });
                await this.mergeParquetFiles(host, sortedFiles, outputPath);

                // Remove original files
                for (const file of sortedFiles) {
                    const filePath = join(this.dataDir, file.path);
                    const dirPath = dirname(filePath);
                    await Bun.file(filePath).delete();
                    
                    try {
                        const remaining = await readdir(dirPath);
                        if (remaining.length === 0) {
                            await Bun.file(dirPath).delete();
                        }
                    } catch (err) {
                        this.log(`Warning: Could not remove directory ${dirPath}: ${err}`);
                    }
                }

                // Create new file metadata
                const stats = {
                    row_count: sortedFiles.reduce((sum, f) => sum + f.row_count, 0),
                    size_bytes: sortedFiles.reduce((sum, f) => sum + f.size_bytes, 0),
                    min_time: Math.min(...sortedFiles.map(f => f.min_time)),
                    max_time: Math.max(...sortedFiles.map(f => f.max_time)),
                    chunk_time: sortedFiles[0].chunk_time
                };

                const newFile = {
                    id: sortedFiles[0].id,
                    path: relativePath,
                    ...stats
                };

                // Update metadata in all snapshot files
                for (const snapshotFile of jsonFiles) {
                    const metadata = await this.readSnapshotMetadata(host, snapshotFile);
                    let metadataChanged = false;

                    for (const [dbId, dbInfo] of metadata.databases) {
                        for (const [tableId, tableFiles] of dbInfo.tables) {
                            // Remove old files and add new one if any were from this hour
                            const hasFilesFromHour = tableFiles.some(f => {
                                const match = f.path.match(/(\d{4}-\d{2}-\d{2})\/(\d{2})/);
                                return match && `${match[1]}_${match[2]}` === hourKey;
                            });

                            if (hasFilesFromHour) {
                                dbInfo.tables.find(t => t[0] === tableId)[1] = [newFile];
                                metadataChanged = true;
                            }
                        }
                    }

                    if (metadataChanged) {
                        const outputPath = join(this.dataDir, host, 'snapshots', snapshotFile);
                        await writeFile(outputPath, JSON.stringify(metadata, null, 2));
                    }
                }

                changed = true;
            }
            
            if (changed) {
                this.log('Compaction completed with changes');
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
