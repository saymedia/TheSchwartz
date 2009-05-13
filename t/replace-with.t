# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 30;

run_tests(10, sub {
    my $client = test_client(dbs => ['ts1']);

    my $handle = $client->insert("Worker::Foo", { cluster => 'all'});
    ok($handle);

    my $job = Worker::Foo->grab_job($client);
    ok($job, "no addition jobs to be grabbed");

    Worker::Foo->work_safely($job);

    $client->can_do("Worker::Foo");
    $client->work_until_done;  # should process 5 jobs.

    # finish a job by replacing it with nothing
    $handle = $client->insert("Worker::Foo", { cluster => 'gibberish'});
    ok($handle->is_pending, "job is still pending");
    $job = $handle->job;
    $job->replace_with();
    ok(! $handle->is_pending, "job no longer pending");

    teardown_dbs('ts1');
});

############################################################################
package Worker::Foo;
use base 'TheSchwartz::Worker';

use Test::More;  ## Import test methods.

sub work {
    my ($class, $job) = @_;
    my $args = $job->arg;

    if ($args->{cluster} eq "all") {
        ok(1, "got the expand job");
        my @jobs;
        for (1..5) {
            push @jobs, TheSchwartz::Job->new_from_array("Worker::Foo",
                    { cluster => $_ }
                );
        }
        # which does a $job->completed iff all the @jobs, in one txn, insert
        # on the same database that $job was on.  and it should DIE if the
        # transaction fails, just so txn flow doesn't proceed on accident.
        # then work_safely with catch the die and call $job->failed
        $job->replace_with(@jobs);
        return;
    }

    if ($args->{cluster} =~ /^\d+$/) {
        ok(1, "got job $args->{cluster}");
        $job->completed;
        return;
    }

    # if anything were to fall through the bottom of here without
    # first calling fail/completed/replace_with, or dying, then the
    # work_safely wrapper should treat it as a "fall-through" failure
    # and log it, doing the whole retries/delay thing as with a
    # regular die.
}

sub grab_for { 30 }

