#!/usr/bin/env perl
use 5.38.0;
use lib qw(lib);

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
