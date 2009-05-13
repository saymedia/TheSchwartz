# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 18;

#use Data::ObjectDriver;
#$Data::ObjectDriver::DEBUG = 1;

run_tests(6, sub {
    my $client = test_client(dbs => ['ts1']);
    my ($job, $handle);

    # insert a job with unique
    $job = TheSchwartz::Job->new(
                                 funcname => 'feed',
                                 uniqkey   => "major",
                                 );
    ok($job, "made first feed major job");
    $handle = $client->insert($job);
    isa_ok $handle, 'TheSchwartz::JobHandle';

    # insert again (notably to same db) and see it fails
    $job = TheSchwartz::Job->new(
                                 funcname => 'feed',
                                 uniqkey  => "major",
                                 );
    ok($job, "made another feed major job");
    $handle = $client->insert($job);
    ok(! $handle, 'no handle');

    # insert same uniqkey, but different func
    $job = TheSchwartz::Job->new(
                                 funcname => 'scratch',
                                 uniqkey   => "major",
                                 );
    ok($job, "made scratch major job");
    $handle = $client->insert($job);
    isa_ok $handle, 'TheSchwartz::JobHandle';

});
