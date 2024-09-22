import fs from 'fs';
import { execSqlite, sort } from './sorter';

function readAllText(path: string): string {
    return fs.readFileSync(path).toString();
}

describe('Sorter', () => {
    beforeAll(() => {
        if(!fs.existsSync('./output')) {
            fs.mkdirSync('./output');
        }
        if(!fs.existsSync('./output/files')) {
            fs.mkdirSync('./output/files');
        }
    });
    beforeEach(() => {
        if(fs.existsSync('./output/files/test.sqlite')) {
            fs.rmSync('./output/files/test.sqlite');
        }
        if(fs.existsSync('./output/files/unordered-id.sorted.csv')) {
            fs.rmSync('./output/files/unordered-id.sorted.csv');
        }
    });
    describe('execSqlite', () => {
        test('should succeed', async () => {
            const script = 'select 2+2;';
            const logs: string[] = [];
            const res = await execSqlite({
                source: { filename: '' },
                destination: { filename: '' },
                sqlite: { filename: './output/files/test.sqlite' },
                schema: [],
                select: [],
                orderBy: [{ name: 'id' }],
                logger: (msg) => { logs.push(msg); },
            }, script);
            expect(res).toBe(0);
            expect(logs).toEqual([
                '[SQLite] 4', 
                '[SQLite] '
            ]);
        });
        test('should fail with unknown function', async () => {
            const logs: string[] = [];
            await expect(async () => {
                const script = 'select unknownfunc(1);';
                const res = await execSqlite({
                    source: { filename: '' },
                    destination: { filename: '' },
                    sqlite: { filename: './output/files/test.sqlite' },
                    schema: [],
                    select: [],
                    orderBy: [{ name: 'id' }],
                    logger: (msg) => { logs.push(msg); },
                }, script);
            }).rejects.toThrow(`no such function: unknownfunc`);
            if (logs.some(x => x.includes('^--- error here'))) {
                // The latest version of SQLite3 enhanced error reporting
                expect(logs).toEqual([
                    '[SQLite] Parse error near line 1: no such function: unknownfunc',
                    '[SQLite]   select unknownfunc(1);',
                    '[SQLite]          ^--- error here',
                    '[SQLite] '
                ]);    
            } else {
                expect(logs).toEqual([
                    '[SQLite] Error: near line 1: in prepare, no such function: unknownfunc (1)',
                    '[SQLite] '          
                ]);    
            }
        });
        test('should fail with unknown sqlite command', async () => {
            await expect(async () => {
                const script = 'select 2+2;';
                const logs: string[] = [];
                const res = await execSqlite({
                    source: { filename: '' },
                    destination: { filename: '' },
                    sqlite: { filename: './output/files/test.sqlite', cli: 'unknown-cli' },
                    schema: [],
                    select: [],
                    orderBy: [{ name: 'id' }],
                    logger: (msg) => { logs.push(msg); },
                }, script);
            }).rejects.toThrowError(`spawn unknown-cli ENOENT`);            
        });
    });
    describe('validations', () => {
        test('source file should exist', async () => {
            await expect(async () => {
                await sort({
                    source: './tests/file-should-not-exist.csv',
                    destination: './output/files/file-should-not-exist.sorted.csv',
                    orderBy: ['id'],
                });    
            }).rejects.toThrowError(`File './tests/file-should-not-exist.csv' does not exist!`);
        });
        test('source destination folder should exist', async () => {
            await expect(async () => {
                await sort({
                    source: './tests/unordered-id.csv',
                    destination: './output/should-not-exist/unordered-id.sorted.csv',
                    orderBy: ['id'],
                });    
            }).rejects.toThrowError(`Folder './output/should-not-exist' does not exist!`);
        });
        test('should not accept empty orderBy option', async () => {
            await expect(async () => {
                await sort({
                    source: './tests/unordered-id.csv',
                    destination: './output/files/unordered-id.sorted.csv',
                    orderBy: [],
                });    
            }).rejects.toThrowError(`You must provide an orderBy option to order the file!`);            
        });
        test('should keep db when specified', async () => {
            if(fs.existsSync('./output/files/unordered-id.sorted.csv')) {
                fs.rmSync('./output/files/unordered-id.sorted.csv');
            }
            expect(fs.existsSync('./output/files/test.sqlite')).toBeFalsy();
            expect(fs.existsSync('./output/files/unordered-id.sorted.csv')).toBeFalsy();
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                orderBy: ['id'],
                sqlite: {
                    filename: './output/files/test.sqlite',
                    keepDB: true,
                }
            });
            expect(fs.existsSync('./output/files/test.sqlite')).toBeTruthy();
            expect(fs.existsSync('./output/files/unordered-id.sorted.csv')).toBeTruthy();

            // sorting when the destination file and the DB exist, should first remove those files
            // when keepDB is false, we should not have the temp DB any more.
            const logs: string[] = [];
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                orderBy: ['id'],
                sqlite: {
                    filename: './output/files/test.sqlite',
                    keepDB: false,
                },
                logger: (msg) => logs.push(msg),
            });
            expect(fs.existsSync('./output/files/test.sqlite')).toBeFalsy();
            expect(fs.existsSync('./output/files/unordered-id.sorted.csv')).toBeTruthy();
            expect(logs).toEqual([
                'Delete destination ./output/files/unordered-id.sorted.csv',
                'Delete SQLite db ./output/files/test.sqlite',
                'Open DB ./output/files/test.sqlite',
                'Execute script:',
                '   .mode csv',
                '   .import "./tests/unordered-id.csv" DATA',
                '   create index DATA_IDX on DATA (id);',
                '   .headers on',
                '   .output "./output/files/unordered-id.sorted.csv"',
                '   select * from DATA order by id;',
                '   .quit',
                'Cleanup',
                'Delete DB ./output/files/test.sqlite'     
            ]);
        });
    });
    describe('orderBy', () => {
        test('one order key', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                orderBy: ['id'],
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,name,age
1,john,12
2,sarah,1
3,mary,2
5,sally,4
6,stan,3
7,paul,33
`);
        });
        test('two order keys', async () => {
            await sort({
                source: './tests/unordered-code-version.csv',
                destination: './output/files/unordered-code-version.sorted.csv',
                orderBy: ['code', 'version'],
            });
            const output = readAllText('./output/files/unordered-code-version.sorted.csv');
            expect(output).toBe(`code,version,name,category,price
abc,1,apple,fruit,0.21
abc,2,apple,fruit,0.20
abc,3,apple,fruit,0.22
def,1,pear,fruit,0.28
def,2,pear,fruit,0.30
def,3,pear,fruit,0.33
ghi,1,beef,meat,3
ghi,2,beef,meat,4
ghi,3,beef,meat,5
`);
            
        });
        test('two order keys and 2nd descending', async () => {
            await sort({
                source: './tests/unordered-code-version.csv',
                destination: './output/files/unordered-code-version.sorted.csv',
                orderBy: [
                    'code', 
                    {
                        name: 'version',
                        sortDirection: 'DESC',
                    },
                ],
            });
            const output = readAllText('./output/files/unordered-code-version.sorted.csv');
            expect(output).toBe(`code,version,name,category,price
abc,3,apple,fruit,0.22
abc,2,apple,fruit,0.20
abc,1,apple,fruit,0.21
def,3,pear,fruit,0.33
def,2,pear,fruit,0.30
def,1,pear,fruit,0.28
ghi,3,beef,meat,5
ghi,2,beef,meat,4
ghi,1,beef,meat,3
`);
            
        });
        test('three order keys and first descending', async () => {
            await sort({
                source: './tests/unordered-code-version.csv',
                destination: './output/files/unordered-code-version.sorted.csv',
                orderBy: [
                    {
                        name: 'category', 
                        sortDirection: 'DESC',
                    },
                    'code', 
                    'version'],
            });
            const output = readAllText('./output/files/unordered-code-version.sorted.csv');
            expect(output).toBe(`code,version,name,category,price
ghi,1,beef,meat,3
ghi,2,beef,meat,4
ghi,3,beef,meat,5
abc,1,apple,fruit,0.21
abc,2,apple,fruit,0.20
abc,3,apple,fruit,0.22
def,1,pear,fruit,0.28
def,2,pear,fruit,0.30
def,3,pear,fruit,0.33
`);
            
        });
        test('should work with special chars in columns, by id', async () => {
            await sort({
                source: './tests/unordered-special-chars.csv',
                destination: './output/files/unordered-special-chars.sorted.csv',
                orderBy: ['id'],
            });
            const output = readAllText('./output/files/unordered-special-chars.sorted.csv');
            expect(output).toBe(`id,"foo bar",f/b,"f'b",f&b,"f ""b"""
1,"a b",a/b,"a'b",a&b,"a ""b"""
2,"a c",a/c,"a'c",a&c,"a ""c"""
3,"a e",a/e,"a'e",a&e,"a ""e"""
`);            
        });
        test('should work with special chars in columns, by "foo bar"', async () => {
            await sort({
                source: './tests/unordered-special-chars.csv',
                destination: './output/files/unordered-special-chars.sorted.csv',
                orderBy: ['foo bar'],
            });
            const output = readAllText('./output/files/unordered-special-chars.sorted.csv');
            expect(output).toBe(`id,"foo bar",f/b,"f'b",f&b,"f ""b"""
1,"a b",a/b,"a'b",a&b,"a ""b"""
2,"a c",a/c,"a'c",a&c,"a ""c"""
3,"a e",a/e,"a'e",a&e,"a ""e"""
`);            
        });
        test('should work with special chars in columns, by "f\'b"', async () => {
            await sort({
                source: './tests/unordered-special-chars.csv',
                destination: './output/files/unordered-special-chars.sorted.csv',
                orderBy: ["f'b"],
            });
            const output = readAllText('./output/files/unordered-special-chars.sorted.csv');
            expect(output).toBe(`id,"foo bar",f/b,"f'b",f&b,"f ""b"""
1,"a b",a/b,"a'b",a&b,"a ""b"""
2,"a c",a/c,"a'c",a&c,"a ""c"""
3,"a e",a/e,"a'e",a&e,"a ""e"""
`);            
        });
        test('should work with special chars in columns, by "f/b"', async () => {
            await sort({
                source: './tests/unordered-special-chars.csv',
                destination: './output/files/unordered-special-chars.sorted.csv',
                orderBy: ['f/b'],
            });
            const output = readAllText('./output/files/unordered-special-chars.sorted.csv');
            expect(output).toBe(`id,"foo bar",f/b,"f'b",f&b,"f ""b"""
1,"a b",a/b,"a'b",a&b,"a ""b"""
2,"a c",a/c,"a'c",a&c,"a ""c"""
3,"a e",a/e,"a'e",a&e,"a ""e"""
`);            
        });
        test('should work with special chars in columns, by "f&b"', async () => {
            await sort({
                source: './tests/unordered-special-chars.csv',
                destination: './output/files/unordered-special-chars.sorted.csv',
                orderBy: ['f&b'],
            });
            const output = readAllText('./output/files/unordered-special-chars.sorted.csv');
            expect(output).toBe(`id,"foo bar",f/b,"f'b",f&b,"f ""b"""
1,"a b",a/b,"a'b",a&b,"a ""b"""
2,"a c",a/c,"a'c",a&c,"a ""c"""
3,"a e",a/e,"a'e",a&e,"a ""e"""
`);            
        });
        test('should work with special chars in columns, by "f ""b"""', async () => {
            await sort({
                source: './tests/unordered-special-chars.csv',
                destination: './output/files/unordered-special-chars.sorted.csv',
                orderBy: ['f "b"'],
            });
            const output = readAllText('./output/files/unordered-special-chars.sorted.csv');
            expect(output).toBe(`id,"foo bar",f/b,"f'b",f&b,"f ""b"""
1,"a b",a/b,"a'b",a&b,"a ""b"""
2,"a c",a/c,"a'c",a&c,"a ""c"""
3,"a e",a/e,"a'e",a&e,"a ""e"""
`);            
        });        
        test('offset wihtout limit should not work', async () => {
            await expect(async () => {
                await sort({
                    source: './tests/unordered-id.csv',
                    destination: './output/files/unordered-id.sorted.csv',
                    orderBy: ['id'],
                    offset: 1,
                });
            }).rejects.toThrowError('You must also specify a limit when using an offset!');
        });

        test('with limit', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                orderBy: ['id'],
                limit: 2,
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,name,age
1,john,12
2,sarah,1
`);
        });

        test('with offset and limit', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                orderBy: ['id'],
                offset: 1,
                limit: 2,
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,name,age
2,sarah,1
3,mary,2
`);
        });

    });
    describe('schema', () => {
        test('should have the same columns', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                schema: ['id', 'name', 'age'],
                orderBy: ['id'],
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,name,age
1,john,12
2,sarah,1
3,mary,2
5,sally,4
6,stan,3
7,paul,33
`);
        });
        test('should specify the age col as numeric and order by age', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                schema: [
                    'id', 
                    'name', 
                    { 
                        name: 'age',
                        type: 'number',
                    },
                ],
                orderBy: ['age'],
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,name,age
2,sarah,1
3,mary,2
6,stan,3
5,sally,4
1,john,12
7,paul,33
`);
        });
        test('should fail if schema does not match inputs', async () => {
            const logs: string[] = []
            await expect(async () => {
                await sort({
                    source: './tests/unordered-id.csv',
                    destination: './output/files/unordered-id.sorted.csv',
                    schema: ['id', 'age'],
                    orderBy: ['id'],
                    logger: msg => logs.push(msg),
                });
            }).rejects.toThrowError(`SQLite error (Exit code = null):
./tests/unordered-id.csv:2: expected 2 columns but found 3 - extras ignored

SQLite command was killed because a column mismatch between schema and inputs was detected.`);
            [
                'Open DB output/files/unordered-id.sorted.sqlite',
                'Execute script:',
                '   CREATE TABLE DATA(',
                '     id TEXT,',
                '     age TEXT',
                '   );',
                '   .mode csv',
                '   .import --skip 1 "./tests/unordered-id.csv" DATA',
                '   create index DATA_IDX on DATA (id);',
                '   .headers on',
                '   .output "./output/files/unordered-id.sorted.csv"',
                '   select * from DATA order by id;',
                '   .quit',
                '[SQLite] ./tests/unordered-id.csv:2: expected 2 columns but found 3 - extras ignored',
                '[SQLite] ',
                'Cleanup',
                'Delete DB output/files/unordered-id.sorted.sqlite'                
            ].forEach(x => expect(logs.includes(x)).toBeTruthy());
        });
    });
    describe('select', () => {
        test('should have only id and age columns in destination', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                select: ['id', 'age'],
                orderBy: ['id'],
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,age
1,12
2,1
3,2
5,4
6,3
7,33
`);
        });
        test('should have all columns in reversed order', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                select: ['age', 'name', 'id'],
                orderBy: ['id'],
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`age,name,id
12,john,1
1,sarah,2
2,mary,3
4,sally,5
3,stan,6
33,paul,7
`);
        });
    });
    describe('where', () => {
        test('where on age, as text', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                orderBy: ['id'],
                where: 'age > "2"',
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,name,age
5,sally,4
6,stan,3
7,paul,33
`);
        });        
        test('where on age, as number', async () => {
            await sort({
                source: './tests/unordered-id.csv',
                destination: './output/files/unordered-id.sorted.csv',
                schema: [
                    'id',
                    'name',
                    { name: 'age', type: 'number' },
                ],
                orderBy: ['id'],
                where: 'age > 2',
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,name,age
1,john,12
5,sally,4
6,stan,3
7,paul,33
`);
        });                
        test('where on a list of categories', async () => {
            await sort({
                source: './tests/unordered-code-version.csv',
                destination: './output/files/unordered-code-version.sorted.csv',
                orderBy: ['code', 'version'],
                where: `category in ('meat', 'cat2', 'cat3')`,
            });
            const output = readAllText('./output/files/unordered-code-version.sorted.csv');
            expect(output).toBe(`code,version,name,category,price
ghi,1,beef,meat,3
ghi,2,beef,meat,4
ghi,3,beef,meat,5
`);
        });                
    });
    describe('formats', () => {
        test('should work with tabs', async () => {
            await sort({
                source: {
                    filename: './tests/unordered-id.tsv',
                    delimiter: '\t',
                },
                destination: {
                    filename: './output/files/unordered-id.sorted.tsv',
                    delimiter: '\t',
                },
                orderBy: ['id'],                
            });
            const output = readAllText('./output/files/unordered-id.sorted.tsv');
            expect(output).toBe(`id	name	age
1	john	12
2	sarah	1
3	mary	2
5	sally	4
6	stan	3
7	paul	33
`);
        });
        test('should work with pipes', async () => {
            await sort({
                source: {
                    filename: './tests/unordered-id.psv',
                    delimiter: '|',
                },
                destination: {
                    filename: './output/files/unordered-id.sorted.psv',
                    delimiter: '|',
                },
                orderBy: ['id'],                
            });
            const output = readAllText('./output/files/unordered-id.sorted.psv');
            expect(output).toBe(`id|name|age
1|john|12
2|sarah|1
3|mary|2
5|sally|4
6|stan|3
7|paul|33
`);
        });
        test('should work with csv as input and tsv as output', async () => {
            await sort({
                source: {
                    filename: './tests/unordered-id.csv',
                },
                destination: {
                    filename: './output/files/unordered-id.sorted.tsv',
                    delimiter: '\t',
                },
                orderBy: ['id'],                
            });
            const output = readAllText('./output/files/unordered-id.sorted.tsv');
            expect(output).toBe(`id	name	age
1	john	12
2	sarah	1
3	mary	2
5	sally	4
6	stan	3
7	paul	33
`);
        });
        test('should work with tsv as input and csv as output', async () => {
            await sort({
                source: {
                    filename: './tests/unordered-id.tsv',
                    delimiter: '\t',
                },
                destination: {
                    filename: './output/files/unordered-id.sorted.csv',
                },
                orderBy: ['id'],                
            });
            const output = readAllText('./output/files/unordered-id.sorted.csv');
            expect(output).toBe(`id,name,age
1,john,12
2,sarah,1
3,mary,2
5,sally,4
6,stan,3
7,paul,33
`);
        });        
    })
});
