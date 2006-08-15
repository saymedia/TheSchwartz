# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 8;

run_tests(4, sub {
    my $client = test_client(dbs => ['ts1']);

    my $job2h;
    for (1..2) {
        my $job = TheSchwartz::Job->new(
                                        funcname => 'Worker::CoalesceTest',
                                        arg      => { n => $_ },
                                        coalesce => "a$_",
                                        );
        my $h = $client->insert($job);
        $job2h = $h if $_ == 2;
        ok($h, "inserted $h");
    }

    $client->reset_abilities;
    $client->can_do("Worker::CoalesceTest");

    my $job = $client->find_job_with_coalescing_prefix("Worker::CoalesceTest", "a1");
    Worker::CoalesceTest->work_safely($job);

    # this one should have succeeded:
    is($job->handle->failures, 0, "no failures on first job");

    # the second one should have failures:
    is($job2h->failures, 1, "1 failure on second job");

    teardown_dbs('ts1');
});

############################################################################
package Worker::CoalesceTest;
use base 'TheSchwartz::Worker';


sub work {
    my ($class, $job) = @_;
    $job->completed;
    my $arg = $job->arg;

    my $job2 = $job->handle->client->find_job_with_coalescing_prefix("Worker::CoalesceTest", "a2");
    $job2->set_as_current;
    die "Failed working on job2\n";
}

sub keep_exit_status_for { 20 }  # keep exit status for 20 seconds after on_complete
sub grab_for { 10 }
sub max_retries { 1 }
sub retry_delay { 10 }


