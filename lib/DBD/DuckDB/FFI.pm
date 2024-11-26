package DBD::DuckDB::FFI;
use strict;
use warnings;
use 5.018;
use Feature::Compat::Try;
use builtin qw( export_lexically );
use experimental 'signatures';

our $VERSION = '0.01';

use FFI::Platypus 2.08;
use FFI::CheckLib qw( find_lib_or_die );
use FFI::C;

my $ffi = FFI::Platypus->new(
    api => 2,
    lib => find_lib_or_die( 
        lib => 'duckdb' ,
        alien => 'Alien::DuckDB'
    ),
);

package DBD::DuckDB::FFI::Database {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1( $ffi, opaque => 'internal_ptr', );
}
$ffi->type( 'record(DBD::DuckDB::FFI::Database)' => 'duckdb_database' );

package DBD::DuckDB::FFI::Connection {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1( $ffi, opaque => 'internal_ptr' );
}
$ffi->type( 'record(DBD::DuckDB::FFI::Connection)' => 'duckdb_connection' );

package DBD::DuckDB::FFI::Result {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1(
        $ffi,
        opaque => 'deprecated_column_count',
        opaque => 'deprecated_row_count',
        opaque => 'deprecated_rows_changed',
        opaque => 'deprecated_columns',
        string => 'deprecated_error_message',
        opaque => 'internal_data'
    );
}
$ffi->type( 'record(DBD::DuckDB::FFI::Result)' => 'duckdb_query_result' );

package DBD::DuckDB::FFI::DataChunk {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1( $ffi, opaque => 'internal_ptr' );
}
$ffi->type( 'record(DBD::DuckDB::FFI::DataChunk)' => 'duckdb_data_chunk' );

package DBD::DuckDB::FFI::Vector {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1( $ffi, opaque => 'internal_ptr' );
}
$ffi->type( 'record(DBD::DuckDB::FFI::Vector)' => 'duckdb_vector' );

package DBD::DuckDB::FFI::Date {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1( $ffi, int => 'days' );
}
$ffi->type( 'record(DBD::DuckDB::FFI::Date)' => 'duckdb_date' );

package DBD::DuckDB::FFI::DateStruct {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1(
        $ffi,
        int => 'year',
        int => 'month',
        int => 'day'
    );
}
$ffi->type( 'record(DBD::DuckDB::FFI::DateStruct)' => 'duckdb_date_struct' );

package DBD::DuckDB::FFI::Time {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1( $ffi, int => 'micros' );
}
$ffi->type( 'record(DBD::DuckDB::FFI::Time)' => 'duckdb_time' );

package DBD::DuckDB::FFI::TimeStruct {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1(
        $ffi,
        int => 'hour',
        int => 'minute',
        int => 'second',
        int => 'micros'
    );
}
$ffi->type( 'record(DBD::DuckDB::FFI::TimeStruct)' => 'duckdb_time_struct' );

#
# TODO duckdb_time_tz && struct
#

package DBD::DuckDB::FFI::Timestamp {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1( $ffi, int => 'micros' );
}
$ffi->type( 'record(DBD::DuckDB::FFI::Timestamp)' => 'duckdb_timestamp' );

package DBD::DuckDB::FFI::TimestampStruct {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1(
        $ffi,
        int => 'year',
        int => 'month',
        int => 'day',
        int => 'hour',
        int => 'minute',
        int => 'second',
        int => 'micros'
    );
}
$ffi->type(
    'record(DBD::DuckDB::FFI::TimestampStruct)' => 'duckdb_timestamp_struct' );

package DBD::DuckDB::FFI::PreparedStatement {
    use FFI::Platypus::Record qw( record_layout_1 );
    record_layout_1( $ffi, opaque => 'internal_ptr' );
}
$ffi->type( 'record(DBD::DuckDB::FFI::PreparedStatement)' =>
      'duckdb_prepared_statement' );

# TODO types
$ffi->type( 'opaque' => 'duckdb_config' );
$ffi->type( 'opaque' => 'duckdb_query_progress_type' );
$ffi->type( 'opaque' => 'duckdb_state' );
$ffi->type( 'opaque' => 'duckdb_type' );
$ffi->type( 'opaque' => 'duckdb_statement_type' );
$ffi->type( 'opaque' => 'duckdb_logical_type' );
$ffi->type( 'opaque' => 'duckdb_result_type' );
$ffi->type( 'opaque' => 'duckdb_result_error_type' );
$ffi->type( 'opaque' => 'duckdb_appender' );
$ffi->type( 'int'    => 'idx_t' );

my %functions = (

    # Startup / Shutdown
    duckdb_open     => [ [ 'string', 'duckdb_database*' ] => 'duckdb_state' ],
    duckdb_open_ext => [
        [ 'string', 'duckdb_database*', 'duckdb_config', 'string*' ] =>
          'duckdb_state'
    ],
    duckdb_close   => [ ['duckdb_database*'] => 'duckdb_state' ],
    duckdb_connect =>
      [ [ 'duckdb_database', 'duckdb_connection*' ] => 'duckdb_state' ],
    duckdb_interrupt      => [ ['duckdb_connection'] => 'void' ],
    duckdb_query_progress =>
      [ ['duckdb_connection'] => 'duckdb_query_progress_type' ],
    duckdb_disconnect      => [ ['duckdb_connection*'] => 'duckdb_state' ],
    duckdb_library_version => [ []                     => 'string' ],

    # Configuration
    duckdb_create_config   => [ ['duckdb_config*'] => 'duckdb_state' ],
    duckdb_config_count    => [ []                 => 'int' ],
    duckdb_get_config_flag =>
      [ [ 'size_t', 'string', 'string' ] => 'duckdb_state' ],
    duckdb_set_config =>
      [ [ 'duckdb_config', 'string', 'string' ] => 'duckdb_state' ],
    duckdb_destroy_config => [ ['duckdb_config*'] => 'duckdb_state' ],

    # Query
    duckdb_query => [
        [ 'duckdb_connection', 'string', 'duckdb_query_result*' ] =>
          'duckdb_state'
    ],
    duckdb_destroy_result => [ ['duckdb_query_result*']          => 'void' ],
    duckdb_column_name    => [ [ 'duckdb_query_result*', 'int' ] => 'string' ],
    duckdb_column_type    =>
      [ [ 'duckdb_query_result*', 'int' ] => 'duckdb_type' ],
    duckdb_result_statement_type =>
      [ ['duckdb_query_result'] => 'duckdb_statement_type' ],
    duckdb_column_logical_type =>
      [ [ 'duckdb_query_result*', 'int' ] => 'duckdb_logical_type' ],

    duckdb_column_count      => [ ['duckdb_query_result*'] => 'idx_t' ],
    duckdb_rows_changed      => [ ['duckdb_query_result*'] => 'idx_t' ],
    duckdb_result_error      => [ ['duckdb_query_result*'] => 'string' ],
    duckdb_result_error_type =>
      [ ['duckdb_query_result*'] => 'duckdb_result_error_type' ],
    duckdb_result_return_type =>
      [ ['duckdb_query_result'] => 'duckdb_result_type' ],

    # data chunks
    duckdb_create_data_chunk =>
      [ [ 'duckdb_logical_type*', 'idx_t' ] => 'duckdb_data_chunk' ],

    duckdb_fetch_chunk => [ ['duckdb_query_result'] => 'duckdb_data_chunk' ],
    duckdb_destroy_data_chunk          => [ ['duckdb_data_chunk*'] => 'void' ],
    duckdb_data_chunk_reset            => [ ['duckdb_data_chunk']  => 'void' ],
    duckdb_data_chunk_get_column_count => [ ['duckdb_data_chunk']  => 'idx_t' ],
    duckdb_data_chunk_get_vector       =>
      [ [ 'duckdb_data_chunk', 'idx_t' ] => 'duckdb_vector' ],
    duckdb_data_chunk_get_size => [ ['duckdb_data_chunk'] => 'idx_t' ],
    duckdb_data_chunk_set_size =>
      [ [ 'duckdb_data_chunk', 'idx_t' ] => 'void' ],
    duckdb_append_data_chunk =>
      [ [ 'duckdb_appender', 'duckdb_data_chunk' ] => 'duckdb_state' ],

    # vectors
    duckdb_vector_get_column_type =>
      [ ['duckdb_vector'] => 'duckdb_logical_type' ],
    duckdb_vector_get_data       => [ ['duckdb_vector']     => 'opaque' ],
    duckdb_vector_get_validity   => [ ['duckdb_vector']     => 'opaque' ],
    duckdb_validity_row_is_valid => [ [ 'opaque', 'idx_t' ] => 'bool' ],

    # Prepared Statements
    duckdb_prepare =>
      [ [ 'duckdb_connection', 'string', 'duckdb_prepared_statement*' ] =>
          'duckdb_state' ],
    duckdb_destroy_prepare => [ ['duckdb_prepared_statement*'] => 'void' ],

    # Utility Functions
    duckdb_bind_boolean => [
        [ 'duckdb_prepared_statement', 'idx_t', 'bool' ] => 'duckdb_state'
    ],
    duckdb_bind_int32 => [
        [ 'duckdb_prepared_statement', 'idx_t', 'int32_t' ] => 'duckdb_state'
    ],
    duckdb_bind_int64 => [
        [ 'duckdb_prepared_statement', 'idx_t', 'int64_t' ] => 'duckdb_state'
    ],
    duckdb_bind_double => [
        [ 'duckdb_prepared_statement', 'idx_t', 'double' ] => 'duckdb_state'
    ],
    duckdb_bind_varchar => [
        [ 'duckdb_prepared_statement', 'idx_t', 'string' ] => 'duckdb_state'
    ],
    duckdb_bind_null => [
        [ 'duckdb_prepared_statement', 'idx_t' ] => 'duckdb_state'
    ],
    duckdb_execute_prepared => [
        [ 'duckdb_prepared_statement', 'duckdb_query_result*' ] =>
          'duckdb_state'
    ],

    duckdb_logical_type_get_alias => [ ['duckdb_logical_type'] => 'string' ],
    duckdb_get_type_id => [ ['duckdb_logical_type'] => 'duckdb_type' ],

    duckdb_vector_size => [ [] => 'idx_t' ],
);

sub cast ( $data, $from, $to ) {
    $ffi->cast( $from, $to, $data );
}

while ( my ( $func, $args ) = each %functions ) {
    try {
        $ffi->attach( $func => $args->@* );
    }
    catch ($e) {
        warn $e;
    }
}

sub import ( $class, @list ) {
    no warnings 'experimental::builtin';
    @list = keys %functions unless @list;
    export_lexically map { $_ => __PACKAGE__->can($_) }
      grep { __PACKAGE__->can($_) } @list;
}

1;
__END__

=head1 NAME

DBD::DuckDB::FFI - DBI driver for DuckDB using FFI

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

    use DBI;
    
    # Connect to an in-memory database
    my $dbh = DBI->connect("dbi:DuckDB:database=:memory:", "", "");
    
    # Create a table
    $dbh->do('CREATE TABLE test (id INTEGER, name TEXT)');
    
    # Insert some data
    $dbh->do('INSERT INTO test VALUES (1, ?), (2, ?)', undef, 'foo', 'bar');
    
    # Query the data
    my $data = $dbh->selectall_hashref('SELECT * FROM test ORDER BY id');
    
    # Work with the results
    for my $row (@$data) {
        printf "ID: %d, Name: %s\n", $row->{id}, $row->{name};
    }
    
    # Disconnect when done
    $dbh->disconnect;

=head1 DESCRIPTION

This module provides a L<DBI> driver for L<DuckDB|https://duckdb.org/>, an embedded 
analytical database. It uses L<FFI::Platypus> to interface with DuckDB's C API, 
allowing you to use DuckDB directly from Perl without compilation.

Key features:

=over 4

=item * No compilation required - uses FFI to interface with DuckDB

=item * Supports both in-memory and file-based databases

=item * Full SQL support including complex analytical queries

=item * Native support for arrays and structured types

=item * High performance for analytical workloads

=back

For implementation details and internal documentation, see L<DBD::DuckDB::FFI::Implementation>.

=head1 USAGE

=head2 Connection

Connect to an in-memory database:

    my $dbh = DBI->connect("dbi:DuckDB:database=:memory:", "", "");

Connect to a file-based database:

    my $dbh = DBI->connect("dbi:DuckDB:database=/path/to/db.duckdb", "", "");

=head2 Data Types

DuckDB supports a wide range of SQL types including:

=over 4

=item * Numeric types (INTEGER, BIGINT, DOUBLE, DECIMAL)

=item * Text types (VARCHAR, TEXT)

=item * Binary types (BLOB)

=item * Date/Time types (DATE, TIME, TIMESTAMP)

=item * Boolean type (BOOLEAN)

=item * Array types (INTEGER[], VARCHAR[], etc.)

=item * Structured types (ROW, MAP)

=back

=head2 Transactions

The driver supports transactions:

    $dbh->{AutoCommit} = 0;
    eval {
        $dbh->do('INSERT INTO test VALUES (1, "test")');
        $dbh->do('UPDATE test SET name = "updated" WHERE id = 1');
        $dbh->commit;
    };
    if ($@) {
        warn "Transaction failed: $@";
        eval { $dbh->rollback };
    }

=head1 SEE ALSO

=over 4

=item * L<DBD::DuckDB> - The main DBI driver module

=item * L<DBI> - Database independent interface for Perl

=item * L<DuckDB Documentation|https://duckdb.org/docs/> - Official DuckDB documentation

=item * L<FFI::Platypus> - FFI interface used by this module

=back

=head1 AUTHOR

Chris Prather, C<< <chris@prather.org> >>

=head1 BUGS

Please report any bugs or feature requests through the web interface at 
L<https://github.com/perigrin/DBD-DuckDB-FFI/issues>.

=head1 COPYRIGHT

Copyright 2024 Chris Prather.

=head1 LICENSE

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
