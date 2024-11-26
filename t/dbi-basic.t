use strict;
use warnings;
use Test::More;
use Test::Deep;
use Test::Exception;
use DBI;

# Test connection
my $dbh;
lives_ok {
    $dbh = DBI->connect("dbi:DuckDB::memory:");
} 'Can connect to in-memory database';

ok($dbh, 'Got a database handle');
isa_ok($dbh, 'DBI::db', 'Database handle is correct class');

# Test basic table operations
lives_ok {
    $dbh->do('CREATE TABLE integers (i INTEGER, j INTEGER);');
} 'Can create a table';

lives_ok {
    $dbh->do('INSERT INTO integers VALUES (3,4), (5,6), (7,NULL);');
} 'Can insert data';

# Test selectall_hashref
my $columns;
lives_ok {
    $columns = $dbh->selectall_hashref('SELECT * FROM integers;', undef);
} 'Can select data using selectall_hashref';

ok($columns && ref($columns) eq 'HASH', 'Got hashref result');
ok(exists $columns->{i} && ref($columns->{i}) eq 'ARRAY', 'Has i column');
ok(exists $columns->{j} && ref($columns->{j}) eq 'ARRAY', 'Has j column');

# Test data content
is_deeply(
    $columns->{i},
    [3, 5, 7],
    'i column has correct values'
);

is_deeply(
    $columns->{j},
    [4, 6, undef],
    'j column has correct values including NULL'
);

# Test output formatting
my $output = '';
{
    open my $fh, '>', \$output or die "Can't open scalar ref: $!";
    local *STDOUT = $fh;
    for (0 .. $columns->{i}->@* - 1) {
        my $i = $columns->{i}[$_] // 'NULL';
        my $j = $columns->{j}[$_] // 'NULL';
        print "$i, $j\n";
    }
}

is(
    $output,
    "3, 4\n5, 6\n7, NULL\n",
    'Output formatting works correctly'
);

# Test disconnection
lives_ok {
    $dbh->disconnect();
} 'Can disconnect from database';

ok(!$dbh->{Active}, 'Database handle is inactive after disconnect');

done_testing();
