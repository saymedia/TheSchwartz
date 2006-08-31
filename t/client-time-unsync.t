# $Id$
# -*-perl-*-
#
# This test tests one client with good time, who grabs a job for 5 seconds.  But while he's
# working on it, another client comes along with a clock set to the future, and grabs the job
# but getting it, since for it, 5 seconds has passed.
#
# This tests that the library doesn't rely on the client's time, but the server's time.
#

use strict;
use warnings;

# make time() be overridable in the future at runtime, rather than be an opcode:
BEGIN { *CORE::GLOBAL::time = sub { time() };  }
no warnings 'redefine';

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 2;

# how we keep track of if job was done twice:  signal from children back up to us
my $got_job = 0;
my $got_done = 0;
$SIG{USR1} = sub { $got_job++; };
$SIG{USR2} = sub { $got_done++; };

# kill children on exit
my %children;  # pid -> 1
my $parent = $$;
END {
    if ($$ == $parent) {
        my @pids = keys %children;
        kill 9, @pids if @pids;
    }
}

run_tests_innodb(2, sub {

    # put one job into database
    my $client = test_client(dbs => ['ts1']);
    $client->insert("Worker::Addition", { numbers => [1, 2] })
        or die;

    # two children to race.  this one with normal time:
    work();

    # let first dude get started first
    select(undef, undef, undef, 1.5);

    # make this worker 60 seconds in the future:  (well past the grabbed until time)
    work(60);

    # hang out waiting for children to finish or timeout
    my $now = time();
    while ($got_done < 2 && time() < $now + 7) {
        sleep 1;
    }

    is($got_done, 2, "two children finished");
    is($got_job, 1, "only did one job");

    teardown_dbs('ts1');
});

sub work {
    my $future = shift;

    # parent:
    if (my $childpid = fork()) {
        $children{$childpid} = 1;
        return;
    }

    if ($future) {
        *CORE::GLOBAL::time = sub { CORE::time() + $future };
    }

    my $client = test_client(dbs => ['ts1'],
                             init => 0);


    # child:
    while (my $job = Worker::Addition->grab_job($client)) {
        eval { Worker::Addition->work($job); };
    }

    kill 'USR2', getppid();
    exit 0;
}

############################################################################
package Worker::Addition;
use base 'TheSchwartz::Worker';

sub work {
    my ($class, $job) = @_;
    sleep 3;
    kill 'USR1', getppid();
    $job->completed;
}

# tell framework to set 'grabbed_until' to time() + 60.  because if
# we can't  add some numbers in 30 seconds, our process probably
# failed and work should be reassigned.
sub grab_for { 5 }

