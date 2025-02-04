import { readdir, mkdir, readFile, writeFile, access } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import duckdb, { DuckDBInstance } from '@duckdb/node-api';

async function removeEmptyDirsUpward(path: string) {
    let current = path;
    while (current) {
        try {
            const files = await readdir(current);
            if (files.length > 0) break; // Stop if directory is not empty
            await Bun.file(current).delete();
            current = dirname(current);
        } catch (err) {
            break; // Stop on any error
        }
    }
}

class ParquetCompactor {
    private dataDir: string;
    private hosts: string[];
    private dryRun: boolean;
    private verbose: boolean;
    private instance: DuckDBInstance | null;
    private connection: any;
    private maxDesiredFileSizeBytes: number;
    private percentageMaxFileSize: number;
    private splitPercentage: number;
    private timeWindowNanos: bigint;

    constructor(dataDir: string, hosts: string[], options: any = {}) {
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
    }

    private log(...args: any[]) {
        if (this.verbose) {
            console.log('[INFO]', ...args);
        }
    }

    private computeCutoffBytes() {
        const small = (this.maxDesiredFileSizeBytes * this.percentageMaxFileSize) / 100;
        const large = (this.maxDesiredFileSizeBytes * (100 + this.percentageMaxFileSize)) / 100;
        return [small, large];
    }

    private normalizePath(path: string) {
        return path.replace(/\/+/g, '/');
    }

    private validateSourcePath(path: string, host: string) {
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

    private isCompactedFile(path: string) {
        return basename(path).startsWith('c_');
    }

    private extractWalSequence(path: string) {
        // Try original format first
        let match = path.match(/(\d{10})\.parquet$/);
        if (match) return match[1];
        
        // Try compacted format
        match = path.match(/c_(\d{10})_\d+_[gh]\d+\.parquet$/);
        if (match) return match[1];
        
        throw new Error(`Could not extract WAL sequence number from path: ${path}`);
    }

    private async mergeParquetFiles(host: string, files: any[], outputPath: string) {
        if (!this.connection) throw new Error('DuckDB connection not initialized');
        
        const filePaths = files.map(f => join(this.dataDir, f.path));
        
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

    private async validateDirectories() {
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

    private async initializeDuckDB() {
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

    private async readSnapshotMetadata(host: string, filename: string) {
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
                
                // Collect all files across all metadata files first, using a Map to deduplicate
                const allFiles = new Map(); // hour -> Map<path, file>
                
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
                                    allFiles.set(key, new Map());
                                }
                                // Use the file path as key to deduplicate
                                allFiles.get(key).set(file.path, file);
                            }
                        }
                    }
                }
                
                // Now process each hour's files
                let changed = false;
                for (const [hourKey, filesMap] of allFiles) {
                    const files = Array.from(filesMap.values());
                    if (files.length <= 1) continue;
                    
                    this.log(`Processing ${files.length} files for hour ${hourKey}`);
                    const [date, hour] = hourKey.split('_');
                    
                    // Sort files by WAL sequence
                    const sortedFiles = files.sort((a, b) => {
                        return parseInt(this.extractWalSequence(a.path)) - 
                               parseInt(this.extractWalSequence(b.path));
                    });

                    // Create compacted file path - fix the base path extraction
                    const parts = sortedFiles[0].path.split('/');
                    if (parts.length < 4) {
                        throw new Error(`Invalid path structure: ${sortedFiles[0].path}`);
                    }
                    
                    const [host, dbsDir, sensorDb, homeDir] = parts;
                    const firstWalSeq = this.extractWalSequence(sortedFiles[0].path);
                    const lastWalSeq = this.extractWalSequence(sortedFiles[sortedFiles.length - 1].path);
                    
                    const outputDir = `${host}/${dbsDir}/${sensorDb}/${homeDir}/${date}/${hour}-00`;
                    const compactedFileName = `c_${firstWalSeq}_${lastWalSeq}_h${hour}.parquet`;
                    const relativePath = join(outputDir, compactedFileName);
                    const outputPath = join(this.dataDir, relativePath);

                    this.log(`Compacting to: ${relativePath}`);
                    this.log('Input files:', sortedFiles.map(f => f.path));

                    // Create directory and merge files
                    await mkdir(dirname(outputPath), { recursive: true });
                    
                    // Ensure all input files exist before attempting merge
                    const existingFiles = [];
                    for (const file of sortedFiles) {
                        const filePath = join(this.dataDir, file.path);
                        try {
                            await access(filePath);
                            existingFiles.push(filePath);
                        } catch (err) {
                            this.log(`Warning: Input file not found: ${filePath}`);
                        }
                    }

                    if (existingFiles.length > 1) {
                        await this.mergeParquetFiles(host, sortedFiles.filter(f => 
                            existingFiles.includes(join(this.dataDir, f.path))
                        ), outputPath);

                        // Remove original files that exist
                        for (const filePath of existingFiles) {
                            try {
                                await Bun.file(filePath).delete();
                                await removeEmptyDirsUpward(dirname(filePath));
                            } catch (err) {
                                this.log(`Warning: Could not remove file ${filePath}: ${err}`);
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

                        // Update metadata
                        changed = true;
                        
                        // Update all metadata files
                        for (const snapshotFile of jsonFiles) {
                            const metadata = await this.readSnapshotMetadata(host, snapshotFile);
                            let metadataChanged = false;

                            for (const [dbId, dbInfo] of metadata.databases) {
                                for (const [tableId, tableFiles] of dbInfo.tables) {
                                    const hasFilesFromHour = tableFiles.some(f => 
                                        existingFiles.includes(join(this.dataDir, f.path))
                                    );
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
                    }
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
