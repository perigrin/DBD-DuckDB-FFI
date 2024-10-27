#!/usr/bin/env perl
use 5.38.0;
use lib './lib';

use DBD::DuckDB::FFI;

my $db = DBD::DuckDB::FFI::Database->new();
duckdb_open( undef, $db );    # undef is an in-memory database
my $con = DBD::DuckDB::FFI::Connection->new();
duckdb_connect( $db, $con );

my $create_table_query =
  'CREATE TABLE IF NOT EXISTS my_table (id INTEGER, name VARCHAR)';
duckdb_query( $con, $create_table_query, undef );

my $insert_query = "INSERT INTO my_table VALUES (1, 'John'), (2, 'Jane')";
duckdb_query( $con, $insert_query, undef );

my $select_query = 'SELECT * FROM my_table';
my $result       = DBD::DuckDB::FFI::Result->new();
duckdb_query( $con, $select_query, $result );

say "Results:";
for my $row ( 0 .. duckdb_row_count($result) - 1 ) {
    my $id   = duckdb_value_int32( $result, 0, $row );
    my $name = duckdb_value_varchar( $result, 1, $row );
    say "$id $name";
}

duckdb_disconnect($con);
duckdb_close($db);
