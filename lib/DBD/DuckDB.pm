use strict;
use warnings;
use 5.018;
use experimental 'signatures';

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

    sub connect ( $drh, $dsn, $user = undef, $auth = undef, $attr = undef ) {
        my ( $outer, $dbh ) = DBI::_new_dbh( $drh, { Name => $dsn } );

        $dbh->{duckdb_db_ptr}   = DBD::DuckDB::FFI::Database->new();
        $dbh->{duckdb_conn_ptr} = DBD::DuckDB::FFI::Connection->new();

        # TODO dsn parsing
        duckdb_open( undef, $dbh->{duckdb_db_ptr} );
        duckdb_connect( $dbh->{duckdb_db_ptr}, $dbh->{duckdb_conn_ptr} );

        # TODO error handling

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
      duckdb_rows_changed
      duckdb_validity_row_is_valid
      duckdb_vector_get_column_type
      duckdb_vector_get_data
      duckdb_vector_get_validity
    );

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
        duckdb_prepare( $dbh->{duckdb_conn_ptr},
            $statement, $sth->{duckdb_st_ptr} );

        # TODO error handling

        return $outer;
    }

    sub do ( $dbh, $statement, $attr=undef, @bind_values ) {
        my $res = DBD::DuckDB::FFI::Result->new();
        duckdb_query( $dbh->{duckdb_conn_ptr}, $statement, $res );
        my $rows = duckdb_rows_changed($res);
        duckdb_destroy_result($res);
        return $rows;
    }

    my @decode = ();
    $decode[4] = sub ( $data, $count ) {    # integer
        DBD::DuckDB::FFI::cast( duckdb_vector_get_data($data),
            'opaque' => "int32_t[$count]" );
    };

    sub selectall_hashref ( $dbh, $sql, $key_field = undef, @params ) {
        my %vectors = ();
        my $res     = DBD::DuckDB::FFI::Result->new();
        duckdb_query( $dbh->{duckdb_conn_ptr}, $sql, $res );
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
    );

    sub execute ( $sth, @bind_values ) {
        my $res = DBD::DuckDB::FFI::Result->new();
        duckdb_execute_prepared( $sth->{duckdb_st_ptr}, $res );
        # TODO: handle bind_values
        return 1;
    }
}

1;
