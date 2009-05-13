# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 6;

run_tests(2, sub {
    my $client = test_client(dbs => ['ts1']);

    my $driver = $client->driver_for( ($client->shuffled_databases)[0] );
    isa_ok $driver, 'Data::ObjectDriver::Driver::DBI';

    cmp_ok $client->get_server_time($driver), '>', 0, 'got server time';

    teardown_dbs('ts1');
});
