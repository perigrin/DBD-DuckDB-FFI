package DBD::DuckDB::FFI;
use 5.26.0;
use warnings;
use Feature::Compat::Try;
use builtin qw( export_lexically );
use experimental 'signatures';

our $VERSION = '0.01';

use FFI::Platypus 2.08;
use FFI::CheckLib qw( find_lib_or_die );
use FFI::C;

my $ffi = FFI::Platypus->new(
    api => 2,
    lib => find_lib_or_die( lib => 'duckdb' ),
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

    duckdb_prepare => [
        [ 'duckdb_connection', 'string', 'duckdb_prepared_statement*' ] =>
          'duckdb_state'
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
