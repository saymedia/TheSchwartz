use strict;
use Test::More;

eval "use Test::Pod::Coverage 1.08";
plan skip_all => "Test::Pod::Coverage 1.08 required for testing POD coverage" if $@;

## Eventually we would be able to test coverage for all modules with
## Test::Pod::all_pod_files(), but let's write the docs first.
my %modules = (
    'TheSchwartz' => {
        also_private => [
            map { qr{ \A $_ \z }xms } qw(
                current_job debug driver_for funcid_to_name funcname_to_id
                handle_from_string hash_databases insert_job_to_driver
                is_database_dead mark_database_as_dead reset_abilities
                restore_full_abilities set_current_job shuffled_databases
                temporarily_remove_ability
            )
        ],
    },
    'TheSchwartz::Worker' => 1,
    'TheSchwartz::Job'    => 1,
);

plan tests => scalar keys %modules;

while (my ($module, $params) = each %modules) {
    pod_coverage_ok($module, ref $params ? $params : ());
}

