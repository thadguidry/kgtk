The ifempty command filters a KGTK file, passing through only those rows for
which one (or more) specified columns contain empty values.  When multiple
columns are specified, there is the choice of requiring any of the columns to
be empty or all of the columns to be empty.

Optionally, report the count of rows that passed the filter instead of
copying the rows to the output file.

## Usage

```
sage: kgtk ifempty [-h] [-i INPUT_FILE] [-o OUTPUT_FILE] --columns FILTER_COLUMN_NAMES
                    [FILTER_COLUMN_NAMES ...] [--count [True|False]] [--all [True|False]]
                    [-v]
                    [INPUT_FILE]

Filter a KGTK file based on whether one or more fields are empty. When multiple fields are specified, either any field or all fields must be empty.

Additional options are shown in expert help.
kgtk --expert ifempty --help

positional arguments:
  INPUT_FILE            The KGTK input file. (May be omitted or '-' for stdin.) (Deprecated,
                        use -i INPUT_FILE)

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input-file INPUT_FILE
                        The KGTK input file. (May be omitted or '-' for stdin.)
  -o OUTPUT_FILE, --output-file OUTPUT_FILE
                        The KGTK output file. (May be omitted or '-' for stdout.)
  --columns FILTER_COLUMN_NAMES [FILTER_COLUMN_NAMES ...]
                        The columns in the file being filtered (Required).
  --count [True|False]  Only count the records, do not copy them. (default=False).
  --all [True|False]    False: Test if any are empty, True: test if all are empty
                        (default=False).

  -v, --verbose         Print additional progress messages (default=False).
```

## Examples

Suppose that `file1.tsv` contains the following table in KGTK format:

| node1 | label   | node2 | location | years |
| ----- | ------- | ----- | -------- | ----- |
| john  | zipcode | 12345 | home     | 10    |
| john  | zipcode | 12346 |          |       |
| peter | zipcode | 12040 | home     |       |
| peter | zipcode | 12040 | work     | 6     |
| steve | zipcode | 45600 |          | 3     |
| steve | zipcode | 45601 | work     |       |

```bash
kgtk ifempty file1.tsv --columns location
```
| node1 | label   | node2 | location | years |
| ----- | ------- | ----- | -------- | ----- |
| john  | zipcode | 12346 |          |       |
| steve | zipcode | 45600 |          | 3     |


```bash
kgtk ifempty file1.tsv --columns years
```

| node1 | label   | node2 | location | years |
| ----- | ------- | ----- | -------- | ----- |
| john  | zipcode | 12346 |          |       |
| peter | zipcode | 12040 | home     |       |
| steve | zipcode | 45601 | work     |       |

```bash
kgtk ifempty file1.tsv --columns location years
```
| node1 | label   | node2 | location | years |
| ----- | ------- | ----- | -------- | ----- |
| john  | zipcode | 12346 |          |       |
| peter | zipcode | 12040 | home     |       |
| steve | zipcode | 45600 |          | 3     |
| steve | zipcode | 45601 | work     |       |

```bash
kgtk ifempty file1.tsv --all --columns location years
```
| node1 | label   | node2 | location | years |
| ----- | ------- | ----- | -------- | ----- |
| john  | zipcode | 12346 |          |       |
