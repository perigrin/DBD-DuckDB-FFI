requires 'perl', '5.018';
requires 'DBI';
requires 'Alien::DuckDB';
requires 'FFI::CheckLib';
requires 'FFI::Platypus' => 2.00;
requires 'FFI::C';
requires 'Feature::Compat::Try';
requires 'builtin::Backport';

on 'configure' => sub {
    requires 'Module::Build::Tiny';
};

on 'test' => sub {
    requires 'Test::More';
    requires 'Test::Deep';
    requires 'Test::Exception';
};
