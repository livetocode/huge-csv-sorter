# Summary

This library can sort huge CSV files efficiently.

Keywords:
- csv
- huge
- large
- big
- sort
- order
- fast
- sqlite

# Why another lib?

Most CSV sorting libraries would read the file in memory for sorting and filtering it, which is not possible when the files are huge!

This library acts as a thin wrapper around the SQLite library and delegates all the work to the DB which is made for this exact scenario.

# Features

- consumes very few memory
- can sort huge files that wouldn't fit in memory
- very fast since it relies on SQLite which is a highly optimized C library

# Requirements

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

sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['code', 'version'],
});
```

### Sort a file with two primary columns, with the one pk in descending order

```Typescript
import { sort } from 'huge-csv-sorter';

sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: [
        'code', 
        {
            name: 'version',
            order: 'DESC',
        }
    ],
});
```

### Sort a file with a subset of the original columns

```Typescript
import { sort } from 'huge-csv-sorter';

sort({
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

sort({
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

sort({
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

sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['id'],
    where: `CATEGORY in ('Cat1', 'Cat2', 'Cat3')`,
});
```

### Sort a file and filter the output rows on a number column

```Typescript
import { sort } from 'huge-csv-sorter';

sort({
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

sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['The ID'],
    where: `"The ID" < 1000`,
});
```

### Sort a file and paginate

```Typescript
import { sort } from 'huge-csv-sorter';

sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['id'],
    offset: 1000,
    limit: 100,
});
```

### Sort a file with custom sqlite settings

```Typescript
import { sort } from 'huge-csv-sorter';

sort({
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

```Typescript
import { sort } from 'huge-csv-sorter';

sort({
    source: 'huge.csv',
    destination: 'huge.sorted.csv',
    orderBy: ['id'],
    logger: console.log,
});
```


