# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 13;

setup_dbs('t/schema-sqlite.sql' => [ 'ts1' ]);

my $client = TheSchwartz->new(databases => [
                                            {
                                                dsn  => dsn_for('ts1'),
                                                user => "",
                                                pass => "",
                                            },
                                            ]);

# insert a job
{
    my $handle = $client->insert("add", { numbers => [1, 2] });
    isa_ok $handle, 'TheSchwartz::JobHandle', "inserted job";
}

# let's do some work.  the tedious way, specifying which class should grab a job
{
    my $job = Worker::Addition->grab_job;
    isa_ok $job, 'TheSchwartz::Job';
    my $args = $job->args;
    is(ref $job, "HASH");  # thawed it for us
    is_deeply($args, { numbers => [1, 2] }, "got our args back");

    # insert a dummy job to test that next grab ignors it
    ok($client->insert("divide", { n => 5, d => 0 }));

    # verify no more jobs can be grabbed of this type, even though
    # we haven't done the first one
    my $job2 = Worker::Addition->grab_job;
    ok(!$job2, "no addition jobs to be grabbed");

    my $rv = eval { Worker::Addition->work($job); };
    # ....
}

# insert some more jobs
{
    ok($client->insert("Worker::MergeInternalDict", { foo => 'bar' }));
    ok($client->insert("Worker::MergeInternalDict", { bar => 'baz' }));
    ok($client->insert("Worker::MergeInternalDict", { baz => 'foo' }));
}

# work the easier way
{
    $client->can("Worker::MergeInternalDict");  # single arg form:  say we can do this job name, which is also its package
    $client->work_until_done;                   # blocks until all databases are empty
    is_deeply(Worker::MergeInternalDict->dict,
              {
                  foo => "bar",
                  bar => "baz",
                  baz => "foo",
              }, "all jobs got completed");
}


teardown_dbs('ts1');

############################################################################
package Worker::Addition;
use base 'TheSchwartz::Worker';

sub handles { "add" }  # the funcnames this class handles.  by default it handles __PACKAGE__

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

sub work {
    my ($class, $job) = @_;
    my $args = $job->args;
    %internal_dict = (%internal_dict, %$args);
    retrn 1;
}

sub grab_for { 10 }
