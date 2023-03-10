# Summary

This library can sort huge CSV files efficiently.

Once your CSV files are properly sorted on a primary key, they can also be efficiently compared to produce a diff file,
using my other lib https://github.com/livetocode/tabular-data-differ

### Keywords
- csv
- huge
- large
- big
- sort
- order
- fast
- sqlite

### Table of content

- [**Why another lib?**](#why-another-lib)
- [**Features**](#features)
- [**Prerequisites**](#prerequisites)
- [**Usage**](#usage)
- [**Documentation**](#documentation)
- [**Development**](#development)

# Why another lib?

Most CSV sorting libraries would read the file in memory for sorting and filtering it, which is not possible when the files are huge!

This library acts as a thin wrapper around the SQLite library and delegates all the work to the DB which is made for this exact scenario.

# Features

- consumes very few memory
- can sort huge files that wouldn't fit in memory
- very fast since it relies on SQLite which is a highly optimized C library

# Prerequisites

The "sqlite3" command must be installed on your system.

For a Mac:
`brew install sqlite`

Don't forget to install the proper package if you're running your app in a container.
For example, using the Node Alpine distro:
`RUN apk add sqlite`

Note that we couldn't use the sqlite npm package since it wouldn't let us execute meta commands such as ".import" which we rely on for importing the CSV.
(see https://sqlite.org/cli.html#csv_import)

# Usage

## Install

`npm i huge-csv-sorter`

## Examples

### Sort a file with one primary column

```Typescript
import { sort } from 'huge-csv-sorter';

sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['id'],
});
```

### Sort a file with two primary columns

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['code', 'version'],
});
```

### Sort a file with two primary columns, with the one pk in descending order

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: [
        'code', 
        {
            name: 'version',
            sortDirection: 'DESC',
        }
    ],
});
```

### Sort a file with a subset of the original columns

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    select: [
        'id',
        'name',
        'price'
    ],
    orderBy: ['id'],
});
```

### Sort a file with typed columns and order by a number column

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    schema: [
        { 
            name: 'id',
            type: 'number',            
        },
        'name',
        {
            name: 'price',
            type: 'number',
        }
    ],
    select: ['id', 'name', 'price'],
    orderBy: ['id'],
});
```

### Sort a file with a custom delimiter such as tab for TSV files

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: {
        filename: 'huge.tsv',
        delimiter: '\t',
    },
    destination: {
        filename: 'huge.sorted.tsv',
        delimiter: '\t',
    },
    orderBy: ['id'],
});
```

### Sort a file and filter the output rows on a text column

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['id'],
    where: `CATEGORY in ('Cat1', 'Cat2', 'Cat3')`,
});
```

### Sort a file and filter the output rows on a number column

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    schema: [
        { 
            name: 'id',
            type: 'number',
        },
        'name',
    ],
    orderBy: ['id'],
    where: `id < 1000`,
});
```

### Sort a file and filter the output rows on a column that must be quoted

Be careful if the name of the columns you're filtering on contain special chars: in this case, you must double-quote them or SQLite will fail to identify the columns.

Note that the where clause should be pure valid SQL and no validation/conversion is done by this library.

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['The ID'],
    where: `"The ID" < 1000`,
});
```

### Sort a file and paginate

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['id'],
    offset: 1000,
    limit: 100,
});
```

### Sort a file with custom sqlite settings

If you want to keep the SQLite database for further inspection after the import, you override the sqlite options.
You can also change the filename of the SQLite database which will use the destination filename and replace the csv extension with sqlite.

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    sqlite: {
        filename: '/tmp/huge.sqlite',
        keepDB: true, // do not delete db after sort
    },
    orderBy: ['id'],
});
```

### Log all commands

If you want to understand how the schema, the import and the query are implemented in SQLite, you can provide your logger function:

```Typescript
import { sort } from 'huge-csv-sorter';

await sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['id'],
    logger: console.log,
});
```

### Order 2 CSV files and diff them on the console

Note that you must also install the diff lib with `npm i tabular-data-differ`.

```Typescript
import { diff } from 'tabular-data-differ';
import { sort } from 'huge-csv-sorter';

await sort({
    source: './tests/a.csv',
    destination: './tests/a.sorted.csv',
    orderBy: ['id'],
});

await sort({
    source: './tests/b.csv',
    destination: './tests/b.sorted.csv',
    orderBy: ['id'],
});

const stats = await diff({
    oldSource: './tests/a.sorted.csv',
    newSource: './tests/b.sorted.csv',
    keys: ['id'],
}).to('console');
console.log(stats);
```

# Documentation

## FileOptions

Name     |Required|Default value|Description
---------|--------|-------------|-----------
filename | yes    |             | a filename
delimiter| no     | ,           | the optional delimiter of the columns

## SchemaColumn

Name     |Required|Default value|Description
---------|--------|-------------|-----------
name     | yes    |             | the name of the column.
type     | no     | string      | the type of the column: either a string or a number.

## SortedColumn

Name         |Required|Default value|Description
-------------|--------|-------------|-----------
name         | yes    |             | the name of the column.
sortDirection| no     | ASC         | the sort direction of the data.

## SQLiteOptions

Name     |Required|Default value|Description
---------|--------|-------------|-----------
filename | yes    |             | the filename of the SQLite temporary database.
keepDB   | no     | false       | specifies whether to keep the database after the operation or if it should be deleted.
cli      | no     | sqlite3     | the SQLite command line tool.

## SortOptions

Name        |Required|Default value|Description
------------|--------|-------------|-----------
source      | yes    |             | either a filename or a FileOptions object
destination | yes    |             | either a filename or a FileOptions object
schema      | no     |             | an optional list of columns annotated with their type (string or number). Note that if is specified, it **must** match all columns of the source file, in the same order of appearance, otherwise the SQLite import will be aborted. 
select      | no     |             | a selection of columns to keep from the source CSV. It will keep all columns when not specified.
orderBy     | yes    |             | a list of columns for ordering the records.
where       | no     |             | the conditions for filtering the records.
offset      | no     | 0           | the offset from which to start selecting the records
limit       | no     |             | the maximum number of records to select. It will keep all records when not specified.
sqlite      | no     |             | options for customizing SQLite.
logger      | no     |             | a function for logging commands sent to SQLite

## sort

The sort function will require a single parameter of type {SortOptions}.

There are only 3 required options:
- source
- destination
- orderBy

# Development

## Install

```shell
git clone git@github.com:livetocode/huge-csv-sorter.git
cd huge-csv-sorter
npm i
```

## Tests

Tests are implemented with Jest and can be run with:
`npm t`

You can also look at the coverage with:
`npm run show-coverage`
