# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 2;

# how we keep track of if job was done twice:  signal from children back up to us
my $work_count = 0;
$SIG{USR1} = sub {
    $work_count++;
};

# force the race condition to happen
{
    no warnings 'once';
    $TheSchwartz::T_AFTER_GRAB_SELECT_BEFORE_UPDATE = sub {
        select undef, undef, undef, 1.5;
    };
}

# kill children on exit
my %children;  # pid -> 1
END {
    my @pids = keys %children;
    kill -9, @pids if @pids;
}

run_tests_innodb(2, sub {

    # get one job into database, to see if children do it twice:
    {
        my $client = test_client(dbs => ['ts1']);
        my $handle = $client->insert("Worker::Addition", { numbers => [1, 2] });
        isa_ok $handle, 'TheSchwartz::JobHandle', "inserted job";
    }

    # two children to race to get the above job.
    work();
    work();

    # hang out for 3 seconds waiting for children to init/race/finish
    my $now = time();
    while (time() < $now + 3) {
        sleep 1;
    }

    is($work_count, 1, "only got one signal from worker children");
    teardown_dbs('ts1');
});

sub work {
    # parent:
    if (my $childpid = fork()) {
        $children{$childpid} = 1;
        return;
    }

    my $client = test_client(dbs => ['ts1'],
                             init => 0);

    # child:
    my $job = Worker::Addition->grab_job($client);
    if ($job) {
        eval { Worker::Addition->work($job); };
    }
    exit 0;
}

############################################################################
package Worker::Addition;
use base 'TheSchwartz::Worker';

sub work {
    my ($class, $job) = @_;
    kill 'USR1', getppid();
    $job->completed;
}

# tell framework to set 'grabbed_until' to time() + 60.  because if
# we can't  add some numbers in 30 seconds, our process probably
# failed and work should be reassigned.
sub grab_for { 30 }

