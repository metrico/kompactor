import { readdir, mkdir, readFile, writeFile } from 'node:fs/promises';
import { join, dirname, resolve } from 'node:path';
import duckdb, { DuckDBInstance, Connection } from '@duckdb/node-api';

class ParquetCompactor {
    private snapshotsDir: string;
    private outputDir: string;
    private dryRun: boolean;
    private verbose: boolean;
    private instance: DuckDBInstance | null = null;
    private connection: Connection | null = null;
    private log: (...args: any[]) => void;

    constructor(snapshotsDir: string, outputDir: string, options: { dryRun?: boolean; verbose?: boolean } = {}) {
        this.snapshotsDir = snapshotsDir;
        this.outputDir = outputDir;
        this.dryRun = options.dryRun || false;
        this.verbose = options.verbose || false;
        
        this.log = this.verbose ? 
            (...args) => console.log('[INFO]', ...args) : 
            () => {};
    }

    private async initializeDuckDB(): Promise<void> {
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

    private async findSnapshotFiles(): Promise<string[]> {
        const files = await readdir(this.snapshotsDir);
        return files.filter(file => file.endsWith('.info.json'));
    }

    private async readSnapshotMetadata(filename: string): Promise<any> {
        const content = await readFile(join(this.snapshotsDir, filename), 'utf-8');
        return JSON.parse(content);
    }

    private async compactParquetFiles(tableFiles: any[], outputPath: string): Promise<void> {
        if (!this.connection) throw new Error('DuckDB connection not initialized');
        
        // Convert file paths to absolute paths
        const filePaths = tableFiles.map(f => resolve(this.snapshotsDir, f.path));
        
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

    private async getParquetStats(filePath: string, sourceFiles?: any[]): Promise<any> {
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

    private async processTableGroup(tableId: number, files: any[]): Promise<any> {
        this.log(`\nProcessing table group ${tableId} with ${files.length} files`);
        this.log('Files to process:', JSON.stringify(files, null, 2));
        
        const outputPath = join(this.outputDir, `table_${tableId}_compacted.parquet`);
        
        if (this.dryRun) {
            this.log(`[DRY-RUN] Would create directory: ${dirname(outputPath)}`);
        } else {
            await mkdir(dirname(outputPath), { recursive: true });
        }

        await this.compactParquetFiles(files, outputPath);
        const stats = await this.getParquetStats(outputPath, files);

        return {
            id: files[0].id,
            path: outputPath,
            size_bytes: stats.size_bytes,
            row_count: stats.row_count,
            chunk_time: files[0].chunk_time,
            min_time: stats.min_time,
            max_time: stats.max_time
        };
    }

    private async updateMetadata(metadata: any, tableId: number, compactedFile: any): Promise<any> {
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

    public async compact(): Promise<void> {
        try {
            await this.initializeDuckDB();
            const snapshotFiles = await this.findSnapshotFiles();

            for (const snapshotFile of snapshotFiles) {
                const metadata = await this.readSnapshotMetadata(snapshotFile);
                
                for (const [dbId, dbInfo] of metadata.databases) {
                    for (const [tableId, files] of dbInfo.tables) {
                        console.log(`Processing table ${tableId} with ${files.length} files...`);
                        
                        const compactedFile = await this.processTableGroup(tableId, files);
                        await this.updateMetadata(metadata, tableId, compactedFile);
                    }
                }

                const outputMetadataPath = join(this.outputDir, snapshotFile);
                if (!this.dryRun) {
                    await writeFile(
                        outputMetadataPath,
                        JSON.stringify(metadata, null, 2)
                    );
                } else {
                    this.log(`[DRY-RUN] Would write updated metadata to: ${outputMetadataPath}`);
                }
            }
        } finally {
            // DuckDB resources will be automatically cleaned up
            this.connection = null;
            this.instance = null;
        }
    }
}

// CLI handling
const usage = `
Usage: bun run parquet-compactor.ts <input-dir> <output-dir> [options]

Arguments:
    input-dir    Directory containing snapshot metadata and parquet files
    output-dir   Directory where compacted files will be written

Options:
    --dry-run    Run without making any changes
    --verbose    Enable detailed logging
    --help       Show this help message

Example:
    bun run parquet-compactor.ts ./snapshots ./compacted --dry-run --verbose
`;

const args = process.argv.slice(2);

if (args.includes('--help') || args.length < 2) {
    console.log(usage);
    process.exit(args.includes('--help') ? 0 : 1);
}

const inputDir = args[0];
const outputDir = args[1];
const dryRun = args.includes('--dry-run');
const verbose = args.includes('--verbose');

console.log(`Starting compactor with:
Input directory:  ${inputDir}
Output directory: ${outputDir}
Dry run: ${dryRun}
Verbose: ${verbose}
`);

const compactor = new ParquetCompactor(inputDir, outputDir, { dryRun, verbose });

try {
    await compactor.compact();
    console.log('Compaction completed successfully!');
} catch (error) {
    console.error('Error during compaction:', error);
    process.exit(1);
}
