use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More;

run_tests(8, sub {
    my $client = test_client(dbs => ['ts1']);

    # insert a job which will fail, fail, then succeed.
    {
        my $handle = $client->insert("Worker::CompleteEventually");
        isa_ok $handle, 'TheSchwartz::JobHandle', "inserted job";

        $client->can_do("Worker::CompleteEventually");
        $client->work_until_done;

        is($handle->failures, 0, "job hasn't failed");
        is($handle->is_pending, 1, "job is still pending");

        my $job = Worker::CompleteEventually->grab_job($client);
        ok(!$job, "a job isn't ready yet"); # hasn't been two seconds
        sleep 3;   # 2 seconds plus 1 buffer second

        $job = Worker::CompleteEventually->grab_job($client);
        ok(!$job, "didn't get a job, because job is 'held' not retrying");
    }

    teardown_dbs('ts1');
});

done_testing;

############################################################################
package Worker::CompleteEventually;
use base 'TheSchwartz::Worker';

sub work {
    my ($class, $job) = @_;
    $job->declined;
    return;
}

sub keep_exit_status_for { 20 }  # keep exit status for 20 seconds after on_complete

sub max_retries { 2 }

sub retry_delay {
    my $class = shift;
    my $fails = shift;
    return [undef,2,0]->[$fails];  # fails 2 seconds first time, then immediately
}

