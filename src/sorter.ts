import fs from 'fs';
import path from 'path';
import { spawn } from 'node:child_process';

/**
 * A logger callback
 */
export type Logger = (message: string) => void;

/**
 * A string representing the filename
 */
export type Filename = string;

/**
 * An object representing file options
 */
export type FileOptions = {
    /**
     * A required string representing the filename
     */
    filename: Filename;
    /**
     * An optional string representing the delimiter of the columns
     * Defaults to comma.
     */
    delimiter?: string;
}

/**
 * A string representing the column name
 */
export type ColumnName = string;

/**
 * A string literal type representing the type of the column
 */
export type ColumnType = 'string' | 'number';

/**
 * An object representing a schema column
 */
export type SchemaColumn = {
    /**
     * A required string representing the name of the column
     */
    name: ColumnName;
    /**
     * An optional string literal type representing the type of the column
     */
    type?: ColumnType;
}

/**
 * A string literal type representing the sort direction of the data
 */
export type SortDirection = 'ASC' | 'DESC';

/**
 * An object representing a sorted column
 */
export type SortedColumn = {
    /**
     * A required string representing the name of the column
     */
    name: ColumnName;
    /**
     * An optional string literal type representing the sort direction of the data
     */
    direction?: SortDirection;
}

/**
 * An interface representing options for SQLite operations
 */
export interface SQLiteOptions {
    /**
     * A required string representing the filename of the SQLite temporary database.
     */
    filename: Filename;
    /**
     * An optional boolean indicating whether to keep the database after the operation or if it should be deleted.
     * Defaults to false.
     */
    keepDB?: boolean;
    /**
     * An optional string representing the SQLite command tool.
     * Defaults to sqlite3.
     */
    cli?: string;
    /**
     * An optional boolean indicating whether to create an index for the database.
     * Defaults to true;
     */
    createIndex?: boolean;
}

/**
 * An interface representing sort options
 */
export interface SortOptions {
    /**
     * A required value representing the source file, either as a Filename or a FileOptions object
     */
    source: Filename | FileOptions;
    /**
     * A required value representing the destination file, either as a Filename or a FileOptions object
     */
    destination: Filename | FileOptions;
    /**
     * An optional array of either ColumnName or SchemaColumn objects representing the schema of the data.
     * If not specified, it will assume that all columns are of type string.
     */
    schema?: (ColumnName | SchemaColumn)[];
    /**
     * An optional array of ColumnName objects representing the columns to select.
     * If not specified, it will select all columns.
     */
    select?: ColumnName[];
    /**
     * A required array of either ColumnName or OrderedColumn objects representing the columns to order by
     */
    orderBy: (ColumnName | SortedColumn)[];
    /**
     * An optional string representing the conditions for filtering the records
     * Note that the where clause should be pure valid SQL and no validation/conversion is done by this library.
     */
    where?: string;
    /**
     * An optional number representing the offset from which to start selecting the records
     */
    offset?: number;
    /**
     * An optional number representing the maximum number of records to select
     */
    limit?: number;
    /**
     * An optional for customizing SQLite operations
     */
    sqlite?: SQLiteOptions;
    /**
     * An optional function for logging commands sent to SQLite
     */
    logger?: Logger;
}

interface SorterOptions {
    source: FileOptions;
    destination: FileOptions;
    schema: SchemaColumn[];
    select: ColumnName[];
    orderBy: SortedColumn[];
    where?: string;
    offset?: number;
    limit?: number;
    sqlite: SQLiteOptions;
    logger: Logger;
}

function convertFileOptions(file: Filename | FileOptions): FileOptions {
    if (typeof file === 'string') {
        return {
            filename: file,            
        };
    }
    return file;
}

function convertSchema(orderBy: string | SchemaColumn): SchemaColumn {
    if (typeof orderBy === 'string') {
        return { name: orderBy };
    }
    return orderBy;
}

function convertSelect(select: string | SchemaColumn): SchemaColumn {
    if (typeof select === 'string') {
        return { name: select };
    }
    return select;
}

function NoopLogger(_message: string) {
}

function defaultSqlLiteFilename(destinationFilename: string) {
    const info = path.parse(destinationFilename);
    return path.join(info.dir, info.name + '.sqlite');
}

function convertOptions(options: SortOptions): SorterOptions {
    const source = convertFileOptions(options.source);
    const destination = convertFileOptions(options.destination);
    return {
        source,
        destination,
        schema: options.schema?.map(convertSelect) ?? [],
        orderBy: options.orderBy?.map(convertSchema),
        select: options.select ?? [],
        where: options.where,
        offset: options.offset,
        limit: options.limit,
        sqlite: options.sqlite ?? { filename: defaultSqlLiteFilename(destination.filename) },
        logger: options.logger ?? NoopLogger,
    };
}

const colMismatchWarning = /expected \d+ columns but found \d+ - extras ignored/;

export function execSqlite(options: SorterOptions, script: string): Promise<number | null> {
    return new Promise<number | null>((resolve, reject) => {
        const errors: string[] = [];
        const command = spawn(options.sqlite.cli ?? 'sqlite3', [options.sqlite.filename]);
    
        command.stdout.on('data', output => {
            const txt: string = output.toString();
            txt.split('\n').map((x) => `[SQLite] ${x}`).forEach(x => options.logger(x));
        });
        command.stderr.on('data', output => {
            const lines: string[] = output.toString().split('\n');
            lines.map(x => `[SQLite] ${x}`).forEach(x => options.logger(x));
            errors.push(...lines);
            const hasMismatch = lines.some(line => colMismatchWarning.test(line));
            if (hasMismatch) {
                errors.push('SQLite command was killed because a column mismatch between schema and inputs was detected.')
                command.kill();                
            }
        });
        command.on('close', function (code) {
            if (code === 0) {
                resolve(code);
            } else {
                reject(new Error(`SQLite error (Exit code = ${code}):\n${errors.slice(0, 20).join('\n')}`));
            }
          });
        command.on('error', function (err) {
            reject(err);
        });
        command.stdin.write(script);
        command.stdin.end();
    });
}

const idValidator = /^[A-Za-z_]([A-Za-z0-9_])*$/;

function isValidIdentifier(id : string) : boolean {
    return idValidator.test(id);
}

function toColumnName(name: string) {
    if (isValidIdentifier(name)) {
        return name;
    }
    const escapedName = name.replaceAll('"', '""');
    return `"${escapedName}"`;
}

/**
 * Sorts an existing CSV file and produces a new file.
 * You must specify the source, destination and orderBy options.
 * @param options specifies the options for sorting a CSV file.
 */
export async function sort(options: SortOptions): Promise<void> {
    const opt = convertOptions(options);
    const sorter = new Sorter();
    return sorter.execute(opt);
}

export class Sorter {
    async execute(options: SorterOptions) {
        this.validate(options);
        const script = this.generateScript(options);
        try {
            await this.executeScript(options, script);
        } finally {
            this.cleanup(options);
        }
    }

    validate(options: SorterOptions) {
        validateFileExists(options.source.filename);
        validateFolderExists(options.destination.filename);
        if (options.sqlite) {
            validateFolderExists(options.sqlite.filename);
        }
        if (options.orderBy.length === 0) {
            throw new Error('You must provide an orderBy option to order the file!');
        }
        if (fs.existsSync(options.destination.filename)) {
            options.logger(`Delete destination ${options.destination.filename}`);
            fs.rmSync(options.destination.filename);
        }
        if (fs.existsSync(options.sqlite.filename)) {
            options.logger(`Delete SQLite db ${options.sqlite.filename}`);
            fs.rmSync(options.sqlite.filename);
        }
        if (options.offset && !options.limit) {
            throw new Error('You must also specify a limit when using an offset!');
        }
    }

    generateScript(options: SorterOptions): string {
        const indexedCols = options.orderBy.map(col => toColumnName(col.name)).join(', ');
        const orderBy = options.orderBy.map(col => `${toColumnName(col.name)} ${col.direction ?? ''}`.trim()).join(', ');
        const lines: string[] = [];

        // Optional schema
        if (options.schema.length > 0) {
            lines.push('CREATE TABLE DATA(')
            for (let i = 0; i < options.schema.length; i++) {
                const col = options.schema[i];
                const colType = col.type === 'number' ? 'NUMERIC' : 'TEXT';
                let colDef = `  ${toColumnName(col.name)} ${colType}`;
                if (i < options.schema.length - 1) {
                    colDef += ',';
                }
                lines.push(colDef);
            }
            lines.push(');')
        }

        // Import source file
        lines.push('.mode csv');
        if (options.source.delimiter) {
            if (options.source.delimiter === '\t') {
                lines.push(`.separator "\t"`);
            } else {
                lines.push(`.separator "${options.source.delimiter}"`);
            }
        }
        const skipFirstRow = options.schema.length > 0 ? '--skip 1 ' : '';
        lines.push(`.import ${skipFirstRow}"${options.source.filename}" DATA`);

        // Optional index for sort
        if (options.sqlite.createIndex !== false) {
            lines.push(`create index DATA_IDX on DATA (${indexedCols});`);
        }

        // Export to destination file
        if (options.destination.delimiter) {
            if (options.destination.delimiter === '\t') {
                lines.push(`.separator "\t"`);
            } else {
                lines.push(`.separator "${options.destination.delimiter}"`);
            }
        } else if (options.source.delimiter) {
            lines.push(`.separator ","`);
        }
        lines.push('.headers on');
        lines.push(`.output "${options.destination.filename}"`);
        let columns = '*';
        if (options.select.length  > 0) {
            columns = options.select.map(toColumnName).join(', ');
        }
        let select = `select ${columns} from DATA`;
        if (options.where) {
            select += ` where ${options.where}`;
        }
        select += ` order by ${orderBy}`;
        if (options.limit) {
            select += ` limit ${options.limit}`;
        }
        if (options.offset) {
            select += ` offset ${options.offset}`;
        }
        lines.push(`${select};`);

        // Done
        lines.push('.quit');
        return lines.join('\n');
    }

    async executeScript(options: SorterOptions, script: string) {
        options.logger(`Open DB ${options.sqlite.filename}`);
        options.logger(`Execute script:`);
        script.split('\n').map(x => `   ${x}`).forEach(x => options.logger(x));
        await execSqlite(options, script);    
    }

    cleanup(options: SorterOptions) {
        options.logger('Cleanup');
        if (options.sqlite.keepDB !== true) {
            options.logger(`Delete DB ${options.sqlite.filename}`);
            fs.rmSync(options.sqlite.filename);
        }
    }
}

function validateFileExists(filename: string): void {
    if(!fs.existsSync(filename)) {
        throw new Error(`File '${filename}' does not exist!`);
    }
}

function validateFolderExists(filename: string): void {
    const folder = path.dirname(filename);
    if (folder && !fs.existsSync(folder)) {
        throw new Error(`Folder '${folder}' does not exist!`);
    }
}