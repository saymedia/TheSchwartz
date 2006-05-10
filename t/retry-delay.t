# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 16;

run_tests(8, sub {
    my $client = test_client(dbs => ['ts1']);

    # insert a job which will fail, fail, then succeed.
    {
        my $handle = $client->insert("Worker::CompleteEventually");
        isa_ok $handle, 'TheSchwartz::JobHandle', "inserted job";

        $client->can_do("Worker::CompleteEventually");
        $client->work_until_done;

        is($handle->failures, 1, "job has failed once");

        my $job = Worker::CompleteEventually->grab_job($client);
        ok(!$job, "a job isn't ready yet"); # hasn't been two seconds
        sleep 3;   # 2 seconds plus 1 buffer second

        $job = Worker::CompleteEventually->grab_job($client);
        ok($job, "got a job, since time has gone by");

        Worker::CompleteEventually->work_safely($job);
        is($handle->failures, 2, "job has failed twice");

        $job = Worker::CompleteEventually->grab_job($client);
        ok($job, "got the job back");

        Worker::CompleteEventually->work_safely($job);
        ok(! $handle->is_pending, "job has exited");
        is($handle->exit_status, 0, "job succeeded");
    }

    teardown_dbs('ts1');
});

############################################################################
package Worker::CompleteEventually;
use base 'TheSchwartz::Worker';

sub work {
    my ($class, $job) = @_;
    my $failures = $job->failures;
    if ($failures < 2) {
        $job->failed;
    } else {
        $job->completed;
    }
    return;
}

sub keep_exit_status_for { 20 }  # keep exit status for 20 seconds after on_complete

sub max_retries { 2 }

sub retry_delay {
    my $class = shift;
    my $fails = shift;
    return [undef,2,0]->[$fails];  # fails 2 seconds first time, then immediately
}

