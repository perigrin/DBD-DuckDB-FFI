#!/usr/bin/env perl
use strict;
use lib qw(lib);
use Test::More;

BEGIN {
    use_ok('DBD::DuckDB::FFI');
}

diag("Testing DBD::DuckDB::FFI $DBD::DuckDB::FFI::VERSION, Perl $], $^X");

done_testing;
