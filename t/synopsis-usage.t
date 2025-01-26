use 5.26.0;
use strict;

use Test::More;

use DBD::DuckDB;
use DBI;

my $dbh = DBI->connect("dbi:DuckDB::memory:");

$dbh->do('CREATE TABLE integers (i INTEGER, j INTEGER);');
$dbh->do('INSERT INTO integers VALUES (3,4), (5,6), (7,NULL);');
my $columns = $dbh->selectall_hashref( 'SELECT * FROM integers;', undef );

my @results = ( [3,4], [5,6], [7,undef] );

subtest "row $_" => sub {
    is $columns->{i}[$_] => $results[$_][0];
    is $columns->{j}[$_] => $results[$_][1];
} for 0 .. $columns->{i}->$#*;

$dbh->disconnect;

done_testing;
