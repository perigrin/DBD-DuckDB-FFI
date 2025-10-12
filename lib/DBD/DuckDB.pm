use strict;
use warnings;
use 5.018;
use experimental 'signatures';

my @decode = ();
$decode[4] = sub ( $data, $count ) {    # integer
    use DBD::DuckDB::FFI qw(duckdb_vector_get_data);

    DBD::DuckDB::FFI::cast( duckdb_vector_get_data($data),
        'opaque' => "int32_t[$count]" );
};

package DBD::DuckDB 0.01 {
    our $VERSION = '0.01';
    our $drh;

    sub driver ( $class, $attr ) {
        return $drh if $drh;

        $class .= '::dr';

        $drh = DBI::_new_drh(
            $class,
            {
                'Name'        => 'DuckDB',
                'Version'     => $__PACKAGE__::VERSION,
                'Attribution' => __PACKAGE__ . ' by Chris Prather',
            }
        );

        return $drh;
    }

    sub CLONE { undef $drh; }
}

package DBD::DuckDB::dr {
    our $imp_data_size = 0;

    use DBD::DuckDB::FFI qw(
      duckdb_open
      duckdb_connect
      duckdb_disconnect
      duckdb_close
    );

    # DuckDB returns 0 (DuckDBSuccess) on success, 1 (DuckDBError) on error
    use constant DUCKDB_SUCCESS => 0;
    use constant DUCKDB_ERROR   => 1;

    sub connect ( $drh, $dsn, $user = undef, $auth = undef, $attr = undef ) {
        # TODO: Parse DSN to extract database path
        my $db_path = undef;  # Default to in-memory
        if ($dsn && $dsn !~ /::memory:$/i) {
            # Extract path from DSN if present
            if ($dsn =~ /database=([^;]+)/) {
                $db_path = $1;
                $db_path = undef if $db_path eq ':memory:';
            }
        }

        # Create database and connection objects
        my $db_ptr   = DBD::DuckDB::FFI::Database->new();
        my $conn_ptr = DBD::DuckDB::FFI::Connection->new();

        # Try to open the database
        my $state = duckdb_open( $db_path, $db_ptr );
        if ($state != DUCKDB_SUCCESS) {
            my $err_msg = "Failed to open database" . ($db_path ? " at $db_path" : " (in-memory)");
            $drh->set_err(1, $err_msg);
            return undef;
        }

        # Try to connect
        $state = duckdb_connect( $db_ptr, $conn_ptr );
        if ($state != DUCKDB_SUCCESS) {
            $drh->set_err(1, "Failed to establish connection");
            duckdb_close($db_ptr);
            return undef;
        }

        # Only create the handle if everything succeeded
        my ( $outer, $dbh ) = DBI::_new_dbh( $drh, { Name => $dsn } );
        $dbh->{duckdb_db_ptr}   = $db_ptr;
        $dbh->{duckdb_conn_ptr} = $conn_ptr;

        return $outer;
    }
}

package DBD::DuckDB::db {
    our $imp_data_size = 0;
    use DBD::DuckDB::FFI qw(
      duckdb_column_name
      duckdb_data_chunk_get_column_count
      duckdb_data_chunk_get_size
      duckdb_data_chunk_get_vector
      duckdb_destroy_data_chunk
      duckdb_destroy_result
      duckdb_disconnect
      duckdb_fetch_chunk
      duckdb_get_row_count
      duckdb_get_type_id
      duckdb_prepare
      duckdb_query
      duckdb_result_error
      duckdb_rows_changed
      duckdb_validity_row_is_valid
      duckdb_vector_get_column_type
      duckdb_vector_get_data
      duckdb_vector_get_validity
    );

    # DuckDB returns 0 (DuckDBSuccess) on success, 1 (DuckDBError) on error
    use constant DUCKDB_SUCCESS => 0;
    use constant DUCKDB_ERROR   => 1;

    sub STORE {
        my ( $dbh, $attr, $val ) = @_;
        if ( $attr eq 'AutoCommit' ) {
            return $val;
        }
        return $dbh->SUPER::STORE( $attr, $val );
    }

    sub FETCH {
        my ( $dbh, $attr ) = @_;
        if ( $attr eq 'AutoCommit' ) {
            return;
        }
        return $dbh->SUPER::FETCH($attr);
    }

    sub prepare ( $dbh, $statement, $attr=undef ) {
        my ( $outer, $sth ) = DBI::_new_sth( $dbh, {} );
        $sth->{duckdb_st_ptr} = DBD::DuckDB::FFI::PreparedStatement->new();

        my $state = duckdb_prepare( $dbh->{duckdb_conn_ptr},
            $statement, $sth->{duckdb_st_ptr} );

        if ($state != DUCKDB_SUCCESS) {
            # Create a temporary result to get error message
            my $res = DBD::DuckDB::FFI::Result->new();
            duckdb_query( $dbh->{duckdb_conn_ptr}, $statement, $res );
            my $err_msg = duckdb_result_error($res) || "Prepare failed";
            duckdb_destroy_result($res);
            $dbh->set_err(1, $err_msg);
            return undef;
        }

        return $outer;
    }

    sub do ( $dbh, $statement, $attr=undef, @bind_values ) {
        my $res = DBD::DuckDB::FFI::Result->new();
        my $state = duckdb_query( $dbh->{duckdb_conn_ptr}, $statement, $res );

        if ($state != DUCKDB_SUCCESS) {
            my $err_msg = duckdb_result_error($res) || "Query failed";
            duckdb_destroy_result($res);
            $dbh->set_err(1, $err_msg);
            return undef;
        }

        my $rows = duckdb_rows_changed($res);
        duckdb_destroy_result($res);

        # Clear any previous errors on success
        $dbh->set_err(undef, undef);

        return $rows;
    }


    sub selectall_hashref ( $dbh, $sql, $key_field = undef, @params ) {
        my %vectors = ();
        my $res     = DBD::DuckDB::FFI::Result->new();
        my $state = duckdb_query( $dbh->{duckdb_conn_ptr}, $sql, $res );

        if ($state != DUCKDB_SUCCESS) {
            my $err_msg = duckdb_result_error($res) || "Query failed";
            duckdb_destroy_result($res);
            $dbh->set_err(1, $err_msg);
            return undef;
        }

        while ( my $chunk = duckdb_fetch_chunk($res) ) {
            my $row_count = duckdb_data_chunk_get_size($chunk);
            last unless $row_count;

            my $col_count = duckdb_data_chunk_get_column_count($chunk);
            last unless $col_count;

            for my $col ( 0 .. $col_count - 1 ) {
                my $name = duckdb_column_name( $res, $col );
                $vectors{$name} //= [];

                my $v = duckdb_data_chunk_get_vector( $chunk, $col );

                my $type =
                  duckdb_get_type_id( duckdb_vector_get_column_type($v) );

                my @data = $decode[$type]->( $v, $row_count )->@*;

                if ( my $mask = duckdb_vector_get_validity($v) ) {
                    push $vectors{$name}->@*, map {
                        duckdb_validity_row_is_valid( $mask, $_ )
                          ? $data[$_]
                          : undef
                    } 0 .. $#data;
                }
                else {
                    push $vectors{$name}->@*, @data;
                }
            }
            duckdb_destroy_data_chunk($chunk);
        }
        duckdb_destroy_result($res);
        return \%vectors;
    }

    sub disconnect ($dbh) {
        if ( my $conn = delete $dbh->{duckdb_conn_ptr} ) {
            duckdb_disconnect($conn);
        }
    }

    sub DESTROY ($dbh) {
        $dbh->disconnect;
        return $dbh->SUPER::DESTROY;
    }
}

package DBD::DuckDB::st {
    our $imp_data_size = 0;

    use DBD::DuckDB::FFI qw(
      duckdb_execute_prepared
      duckdb_result_error
      duckdb_rows_changed duckdb_validity_row_is_valid
      duckdb_vector_get_column_type duckdb_vector_get_data
      duckdb_vector_get_validity duckdb_fetch_chunk duckdb_data_chunk_get_size
      duckdb_data_chunk_get_column_count duckdb_column_name
      duckdb_data_chunk_get_vector duckdb_get_type_id duckdb_destroy_data_chunk
      duckdb_destroy_result
    );

    # DuckDB returns 0 (DuckDBSuccess) on success, 1 (DuckDBError) on error
    use constant DUCKDB_SUCCESS => 0;
    use constant DUCKDB_ERROR   => 1;

    my sub fetch_vectors ($res) {
        my %vectors = ();
        while ( my $chunk = duckdb_fetch_chunk($res) ) {
            my $row_count = duckdb_data_chunk_get_size($chunk);
            last unless $row_count;

            my $col_count = duckdb_data_chunk_get_column_count($chunk);
            last unless $col_count;

            for my $col ( 0 .. $col_count - 1 ) {
                my $name = duckdb_column_name( $res, $col );
                $vectors{$name} //= [];

                my $v = duckdb_data_chunk_get_vector( $chunk, $col );

                my $type =
                  duckdb_get_type_id( duckdb_vector_get_column_type($v) );

                my @data = $decode[$type]->( $v, $row_count )->@*;

                if ( my $mask = duckdb_vector_get_validity($v) ) {
                    push $vectors{$name}->@*, map {
                        duckdb_validity_row_is_valid( $mask, $_ )
                          ? $data[$_]
                          : undef
                    } 0 .. $#data;
                }
                else {
                    push $vectors{$name}->@*, @data;
                }
            }
            duckdb_destroy_data_chunk($chunk);
        }
        return \%vectors;
    }

    sub execute ( $sth, @bind_values ) {
        $sth->{res} = DBD::DuckDB::FFI::Result->new();
        my $state = duckdb_execute_prepared( $sth->{duckdb_st_ptr}, $sth->{res} );

        # TODO: handle bind_values

        if ($state != DUCKDB_SUCCESS) {
            my $err_msg = duckdb_result_error($sth->{res}) || "Execute failed";
            $sth->set_err(1, $err_msg);
            return undef;
        }

        # Clear any previous errors on success
        $sth->set_err(undef, undef);

        return 1;
    }
    sub fetchall_arrayref ( $sth, $attr = undef ) {
        return [ fetch_vectors($sth->{res})];
    }

    sub fetchall_hashref ( $sth, $attr = undef ) {
        return { fetch_vectors($sth->{res})->%* };
    }
}


1;
