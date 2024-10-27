package DBD::DuckDB::FFI;
use 5.26.0;
use warnings;
use feature 'signatures';
use Feature::Compat::Try;
use builtin qw( export_lexically );

our $VERSION = '0.01';

use FFI::Platypus 2.08;
use FFI::CheckLib qw( find_lib_or_die );

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

$ffi->type( 'opaque' => 'duckdb_state' );

my %functions = (
    duckdb_open    => [ [ 'string', 'duckdb_database*' ] => 'duckdb_state' ],
    duckdb_connect =>
      [ [ 'duckdb_database', 'duckdb_connection*' ] => 'duckdb_state' ],
    duckdb_query => [
        [ 'duckdb_connection', 'string', 'duckdb_query_result*' ] =>
          'duckdb_state'
    ],

    duckdb_row_count   => [ ['duckdb_query_result*']                 => 'int' ],
    duckdb_value_int32 => [ [ 'duckdb_query_result*', 'int', 'int' ] => 'int' ],
    duckdb_value_varchar =>
      [ [ 'duckdb_query_result*', 'int', 'int' ] => 'string' ],
    duckdb_disconnect => [ ['duckdb_connection*'] => 'duckdb_state' ],
    duckdb_close      => [ ['duckdb_database*']   => 'duckdb_state' ],
);

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
