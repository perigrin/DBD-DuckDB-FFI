#!/usr/bin/env perl
use 5.38.0;
use lib './lib';
use builtin qw(true);
no warnings 'experimental::builtin';

use DBD::DuckDB::FFI;

my $db = DBD::DuckDB::FFI::Database->new();
duckdb_open( undef, $db );    # undef is an in-memory database
my $con = DBD::DuckDB::FFI::Connection->new();
duckdb_connect( $db, $con );

my $res = DBD::DuckDB::FFI::Result->new();
duckdb_query( $con, 'CREATE TABLE integers (i INTEGER, j INTEGER);', undef );
duckdb_query( $con, 'INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);',
    undef );
duckdb_query( $con, 'SELECT * From integers;', $res );

while (true) {
    my $result = duckdb_fetch_chunk($res);
    last unless $result;

    my $row_count = duckdb_data_chunk_get_size($result);
    last unless $row_count;

    my $col1      = duckdb_data_chunk_get_vector( $result, 0 );
    my $col1_data = DBD::DuckDB::FFI::cast( duckdb_vector_get_data($col1),
        'opaque' => "int32_t[$row_count]" );
    my $col1_validity = duckdb_vector_get_validity($col1);

    my $col2      = duckdb_data_chunk_get_vector( $result, 1 );
    my $col2_data = DBD::DuckDB::FFI::cast( duckdb_vector_get_data($col2),
        'opaque' => "int32_t[$row_count]" );
    my $col2_validity = duckdb_vector_get_validity($col2);

    for my $row ( 0 .. $row_count - 1 ) {
        if ( duckdb_validity_row_is_valid( $col1_validity, $row ) ) {
            my $data = $col1_data->[$row];
            print $data;
        }
        else {
            print 'NULL';
        }
        print ',';
        if ( duckdb_validity_row_is_valid( $col2_validity, $row ) ) {
            my $data = $col2_data->[$row];
            print $data;
        }
        else {
            print 'NULL';
        }
        print "\n";
    }
    duckdb_destroy_data_chunk($result);
}

duckdb_destroy_result($res);
duckdb_disconnect($con);
duckdb_close($db);
