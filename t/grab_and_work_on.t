# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 18;

run_tests(9, sub {
    my $client = test_client(dbs => ['ts1']);

    my $available = TheSchwartz::Job->new(
        funcname => 'Worker::Grabber',
    );
    my $grabbed_until = time + 2;
    my $grabbed = TheSchwartz::Job->new(
        funcname => 'Worker::Grabber',
        grabbed_until => $grabbed_until,
    );
    my $available_handle = $client->insert($available);
    my $grabbed_handle   = $client->insert($grabbed);

    $client->reset_abilities;
    $client->can_do("Worker::Grabber");

    Worker::Grabber->set_client($client);

    my $rv = $client->grab_and_work_on($grabbed_handle->as_string);
    ok(!$rv, "we couldn't grab it");
    is scalar $grabbed->failure_log, 0, "no errors";
    $grabbed->refresh;
    is $grabbed->grabbed_until, $grabbed_until, "Still grabbed";

    $rv = $client->grab_and_work_on($available_handle->as_string);
    is scalar $available->failure_log, 0, "no errors";
    ok($rv, "we worked on it");
    
    $rv = $client->grab_and_work_on($available_handle->as_string);
    is scalar $available->failure_log, 0, "no errors";
    ok(!$rv, "There is nothing to do for it now.");
    
    teardown_dbs('ts1');
});

############################################################################
package Worker::Grabber;
use base 'TheSchwartz::Worker';
use Test::More;

my $client;
sub set_client { $client = $_[1]; }

sub work {
    my ($class, $job) = @_;

    ok(($job->grabbed_until > time), "this job is locked");

    ## try to work on it
    my $rv =  $client->grab_and_work_on($job->handle->as_string);
    ok(!$rv, "We are already working on it, so we can't grab it");
    
    $job->completed;
}
