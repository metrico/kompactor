import { readdir, mkdir, readFile, writeFile, access } from 'node:fs/promises';
import { join, dirname, resolve, sep, basename } from 'node:path';
import duckdb, { DuckDBInstance, Connection } from '@duckdb/node-api';

class ParquetCompactor {
    dataDir: string;
    hosts: string[];
    dryRun: boolean;
    verbose: boolean;
    instance: DuckDBInstance | null = null;
    connection: Connection | null = null;
    log: (...args: any[]) => void;

    constructor(dataDir: string, hosts: string[], options: { dryRun?: boolean; verbose?: boolean } = {}) {
        this.dataDir = dataDir;
        this.hosts = hosts;
        this.dryRun = options.dryRun || false;
        this.verbose = options.verbose || false;
        
        this.log = this.verbose ? 
            (...args) => console.log('[INFO]', ...args) : 
            () => {};
    }

    getHostDir(host: string) {
        return join(this.dataDir, host);
    }

    getSnapshotsDir(host: string) {
        return join(this.getHostDir(host), 'snapshots');
    }

    getDbsDir(host: string) {
        return join(this.getHostDir(host), 'dbs');
    }

    async validateDirectories() {
        // Check if data directory exists
        try {
            await access(this.dataDir);
        } catch {
            throw new Error(`Data directory does not exist: ${this.dataDir}`);
        }

        // Check if each host directory exists
        for (const host of this.hosts) {
            const hostDir = this.getHostDir(host);
            try {
                await access(hostDir);
            } catch {
                throw new Error(`Host directory does not exist: ${hostDir}`);
            }

            // Check for snapshots directory
            try {
                await access(this.getSnapshotsDir(host));
            } catch {
                throw new Error(`Snapshots directory does not exist: ${this.getSnapshotsDir(host)}`);
            }

            // Check for dbs directory
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
            threads: '4'  // Use 4 threads for parallel processing
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

    async compactParquetFiles(host: string, tableFiles: any[], outputPath: string) {
        if (!this.connection) throw new Error('DuckDB connection not initialized');
        
        // Convert file paths to absolute paths, ensuring proper host directory is used
        const filePaths = tableFiles.map(f => join(this.getHostDir(host), f.path));
        
        this.log('Compacting files:', filePaths);
        this.log('Output path:', outputPath);
        
        if (this.dryRun) {
            this.log('[DRY-RUN] Would execute DuckDB query to compact files');
            return;
        }

        // Prepare the file list for the query
        const fileListQuery = filePaths.map(path => `'${path}'`).join(', ');
        await this.connection.run(`
            COPY (
                SELECT * 
                FROM read_parquet([${fileListQuery}])
                ORDER BY min_time
            ) 
            TO '${outputPath}' (FORMAT 'parquet')
        `);
    }

    async getParquetStats(filePath: string, sourceFiles?: any[]) {
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

    async processTableGroup(host: string, tableId: number, files: any[]) {
        this.log(`\nProcessing table group ${tableId} with ${files.length} files`);
        this.log('Files to process:', JSON.stringify(files, null, 2));
        
        // Use the first file's path components to build the output path
        const firstFile = files[0];
        const baseFileName = basename(firstFile.path); // Using imported basename directly
        const firstFilePath = firstFile.path;
        
        // The output path should exactly match the input path format
        const outputPath = join(
            this.getDbsDir(host),
            ...firstFilePath.split('/').slice(2) // Remove 'my_host/dbs' prefix as it's added by getDbsDir
        );
        
        if (this.dryRun) {
            this.log(`[DRY-RUN] Would create directory: ${dirname(outputPath)}`);
        } else {
            await mkdir(dirname(outputPath), { recursive: true });
        }

        await this.compactParquetFiles(host, files, outputPath);
        const stats = await this.getParquetStats(outputPath, files);

        // Keep the exact same path format as input
        const relativePath = firstFilePath;

        return {
            id: firstFile.id,
            path: relativePath,
            size_bytes: stats.size_bytes,
            row_count: stats.row_count,
            chunk_time: firstFile.chunk_time,
            min_time: stats.min_time,
            max_time: stats.max_time
        };
    }

    async updateMetadata(metadata: any, tableId: number, compactedFile: any) {
        this.log('\nUpdating metadata for table:', tableId);
        this.log('Compacted file info:', JSON.stringify(compactedFile, null, 2));

        for (const [dbId, dbInfo] of metadata.databases) {
            for (const [tId, files] of dbInfo.tables) {
                if (tId === tableId) {
                    this.log(`Found matching table ${tId} in database ${dbId}`);
                    this.log('Original files:', JSON.stringify(files, null, 2));
                    
                    if (this.dryRun) {
                        this.log('[DRY-RUN] Would replace files with:', JSON.stringify([compactedFile], null, 2));
                    } else {
                        dbInfo.tables.set(tId, [compactedFile]);
                    }
                }
            }
        }

        if (this.dryRun) {
            this.log('[DRY-RUN] Would update metadata statistics to:', {
                parquet_size_bytes: compactedFile.size_bytes,
                row_count: compactedFile.row_count,
                min_time: compactedFile.min_time,
                max_time: compactedFile.max_time
            });
        } else {
            metadata.parquet_size_bytes = compactedFile.size_bytes;
            metadata.row_count = compactedFile.row_count;
            metadata.min_time = compactedFile.min_time;
            metadata.max_time = compactedFile.max_time;
        }

        return metadata;
    }

    async readSnapshotMetadata(host: string, filename: string) {
        const content = await readFile(join(this.getSnapshotsDir(host), filename), 'utf-8');
        return JSON.parse(content);
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
                            
                            const compactedFile = await this.processTableGroup(host, tableId, files);
                            await this.updateMetadata(metadata, tableId, compactedFile);
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

const compactor = new ParquetCompactor(dataDir, hosts, { dryRun, verbose });

try {
    await compactor.compact();
    console.log('Compaction completed successfully!');
} catch (error) {
    console.error('Error during compaction:', error);
    process.exit(1);
}
