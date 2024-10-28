# DBD::DuckDB

A DBI driver for DuckDB.

## WARNING

This is a work in progress. Please use at your own risk. The barest
functionality is implemented in the FFI wrapper and the DBD interface.

Proceed at your own risk.

## SYNOPSIS

```perl
use DBD::DuckDB;
use DBI;

my $dbh = DBI->connect("dbi:DuckDB::memory:");

$dbh->do('CREATE TABLE integers (i INTEGER, j INTEGER);');
$dbh->do('INSERT INTO integers VALUES (3,4), (5,6), (7,NULL);');
my $columns = $dbh->selectall_hashref( 'SELECT * FROM integers;', undef );

for ( 0 .. $columns->{i}->@* - 1 ) {
    my $i = $columns->{i}[$_] // 'NULL';
    my $j = $columns->{j}[$_] // 'NULL';
    say "$i, $j";
}
$dbh->disconnect();
```

## Description

This is a DBI driver and a FFI wrapper around the DuckDB database engine.
