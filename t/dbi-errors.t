use strict;
use warnings;
use Test::More;
use Test::Exception;
use DBI;

# Test error handling for connection failures
{
    my $dbh;
    dies_ok {
        $dbh = DBI->connect("dbi:DuckDB:database=/nonexistent/path/to/db.duckdb", "", "", {RaiseError => 1});
    } 'Connection to invalid path should die with RaiseError';

    ok(!defined $dbh || !$dbh, 'Database handle should not be valid after connection error');
}

# Test error handling with RaiseError off
{
    my $dbh = DBI->connect("dbi:DuckDB:database=/nonexistent/path/to/db.duckdb", "", "", {RaiseError => 0, PrintError => 0});
    ok(!defined $dbh || !$dbh, 'Connection should fail for invalid path');
    ok($DBI::errstr, 'DBI::errstr should contain error message');
}

# Test error handling for invalid SQL
{
    my $dbh = DBI->connect("dbi:DuckDB::memory:", "", "", {RaiseError => 1});
    ok($dbh, 'Can connect to in-memory database');

    dies_ok {
        $dbh->do('INVALID SQL SYNTAX HERE');
    } 'Invalid SQL should die with RaiseError';

    ok($dbh->err, 'err attribute should be set after error');
    ok($dbh->errstr, 'errstr attribute should contain error message');
    like($dbh->errstr, qr/syntax|parser|error/i, 'Error message should mention syntax or parser error');
}

# Test error handling with RaiseError off
{
    my $dbh = DBI->connect("dbi:DuckDB::memory:", "", "", {RaiseError => 0, PrintError => 0});
    ok($dbh, 'Can connect to in-memory database');

    my $result = $dbh->do('INVALID SQL SYNTAX HERE');
    ok(!defined $result, 'do() should return undef on error');
    ok($dbh->err, 'err attribute should be set');
    ok($dbh->errstr, 'errstr attribute should contain error message');
}

# Test error handling for prepare failures
{
    my $dbh = DBI->connect("dbi:DuckDB::memory:", "", "", {RaiseError => 1});
    ok($dbh, 'Can connect to in-memory database');

    dies_ok {
        $dbh->prepare('SELECT * FROM nonexistent_table');
    } 'Prepare with nonexistent table should die with RaiseError';

    ok($dbh->err, 'err should be set after prepare error');
    ok($dbh->errstr, 'errstr should contain error message');
}

# Test error handling for execute failures
{
    my $dbh = DBI->connect("dbi:DuckDB::memory:", "", "", {RaiseError => 0, PrintError => 0});
    ok($dbh, 'Can connect to in-memory database');

    $dbh->do('CREATE TABLE test (id INTEGER)');
    my $sth = $dbh->prepare('INSERT INTO test VALUES (?)');
    ok($sth, 'Can prepare statement');

    # This should work (but requires bind parameters - issue #4)
    TODO: {
        local $TODO = 'Bind parameters not yet implemented (issue #4)';
        my $result = $sth->execute(42);
        ok($result, 'Execute should succeed with valid data');
    }
}

# Test that successful operations clear errors
{
    my $dbh = DBI->connect("dbi:DuckDB::memory:", "", "", {RaiseError => 0, PrintError => 0});
    ok($dbh, 'Can connect to in-memory database');

    # Cause an error
    $dbh->do('INVALID SQL');
    ok($dbh->err, 'err should be set after error');

    # Successful operation should clear error
    $dbh->do('CREATE TABLE test (id INTEGER)');
    ok(!$dbh->err, 'err should be cleared after successful operation');
}

done_testing();
