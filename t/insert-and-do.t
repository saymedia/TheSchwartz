# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 52;

run_tests(26, sub {
    my $client = test_client(dbs => ['ts1']);

    # insert a job
    {
        my $handle = $client->insert("Worker::Addition", { numbers => [1, 2] });
        isa_ok $handle, 'TheSchwartz::JobHandle', "inserted job";
    }

    # let's do some work.  the tedious way, specifying which class should grab a job
    {
        my $job = Worker::Addition->grab_job($client);
        isa_ok $job, 'TheSchwartz::Job';
        my $args = $job->arg;
        is(ref $args, "HASH");  # thawed it for us
        is_deeply($args, { numbers => [1, 2] }, "got our args back");

        # insert a dummy job to test that next grab ignors it
        ok($client->insert("dummy", [1,2,3]));

        # verify no more jobs can be grabbed of this type, even though
        # we haven't done the first one
        my $job2 = Worker::Addition->grab_job($client);
        ok(!$job2, "no addition jobs to be grabbed");

        my $rv = eval { Worker::Addition->work($job); };
        # ....
    }

    # inserting and getting job w/ regular scalar arg
    foreach my $scalar ("short_arg",
                        "long arg more than 11 bytes long",
                        "\x05scalar that begins with the 5 byte",
                        )
    {
        my $handle = $client->insert("Worker::Addition", $scalar);
        isa_ok $handle, 'TheSchwartz::JobHandle', "inserted job";

        my $job = Worker::Addition->grab_job($client);
        isa_ok $job, 'TheSchwartz::Job';
        my $args = $job->arg;
        ok(!ref $args, "not a reference");  # not a reference
        is($args, $scalar, "got correct scalar arg back");
    }

    # insert some more jobs
    {
        ok($client->insert("Worker::MergeInternalDict", { foo => 'bar' }));
        ok($client->insert("Worker::MergeInternalDict", { bar => 'baz' }));
        ok($client->insert("Worker::MergeInternalDict", { baz => 'foo' }));
    }

    # work the easier way
    {
        Worker::MergeInternalDict->reset;
        $client->can_do("Worker::MergeInternalDict");  # single arg form:  say we can do this job name, which is also its package
        $client->work_until_done;                   # blocks until all databases are empty
        is_deeply(Worker::MergeInternalDict->dict,
                  {
                      foo => "bar",
                      bar => "baz",
                      baz => "foo",
                  }, "all jobs got completed");
    }

    # errors
    {
        $client->reset_abilities;           # now it, as a worker, can't do anything
        $client->can_do("Worker::Division");   # now it can only do one thing

        my $handle = $client->insert("Worker::Division", { n => 5, d => 0 });
        ok($handle);

        my $job = Worker::Division->grab_job($client);
        isa_ok $job, 'TheSchwartz::Job';

        # wrapper around 'work' implemented in the base class which runs work in
        # eval and notes a failure (with backoff) if job died.
        Worker::Division->work_safely($job);

        is($handle->failures, 1, "job has failed once");
        like(join('', $handle->failure_log), qr/Illegal division by zero/, "noted that we divided by zero");
    }

    teardown_dbs('ts1');
});

############################################################################
package Worker::Addition;
use base 'TheSchwartz::Worker';

sub work {
    my ($class, $job) = @_;

    # ....
}

# tell framework to set 'grabbed_until' to time() + 60.  because if
# we can't  add some numbers in 30 seconds, our process probably
# failed and work should be reassigned.
sub grab_for { 30 }

############################################################################
package Worker::MergeInternalDict;
use base 'TheSchwartz::Worker';
my %internal_dict;

sub reset { %internal_dict = (); }

sub dict { \%internal_dict }

sub work {
    my ($class, $job) = @_;
    my $args = $job->arg;
    %internal_dict = (%internal_dict, %$args);
    $job->completed;
}

sub grab_for { 10 }

############################################################################
package Worker::Division;
use base 'TheSchwartz::Worker';

sub work {
    my ($class, $job) = @_;
    my $args = $job->arg;

    my $ans = $args->{n} / $args->{d};  # throw it away, just here to die on d==0

    $job->set_exit_status(1);
    $job->completed;
}

sub keep_exit_status_for { 20 }  # keep exit status for 20 seconds after on_complete

sub grab_for { 10 }

sub max_retries { 1 }

sub retry_delay { my $class = shift; my $fails = shift; return 2 ** $fails; }

