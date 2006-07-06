# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 2;

# how we keep track of if job was done twice:  signal from children back up to us
my $work_count = 0;
my $lost_race  = 0;
$SIG{USR1} = sub { $work_count++; };
$SIG{USR2} = sub { $lost_race ++; };

# tell our parent when we lost a race
{
    no warnings 'once';
    $TheSchwartz::FIND_JOB_BATCH_SIZE = 2;

    $TheSchwartz::T_LOST_RACE = sub {
        $lost_race = 1;  # this one's in our child process.
        kill 'USR2', getppid();
    };

    $TheSchwartz::T_AFTER_GRAB_SELECT_BEFORE_UPDATE = sub {
        # force the race condition to happen, at least until we've triggered it
        select undef, undef, undef, 0.25
            unless $lost_race;
    };

}

# kill children on exit
my %children;  # pid -> 1
END {
    my @pids = keys %children;
    kill -9, @pids if @pids;
}

my $jobs = 40;

run_tests_innodb(2, sub {

    # get one job into database, to see if children do it twice:
    {
        my $client = test_client(dbs => ['ts1']);
        for (1..$jobs) {
            $client->insert("Worker::Addition", { numbers => [1, 2] })
                or die;
        }
    }

    # two children to race
    work();
    work();

    # hang out waiting for children to init/race/finish
    #
    while ($work_count < $jobs) {
        sleep 1;
    }
    my $now = time();
    while (time < $now + 2) {
        sleep 1;
    }

    is($work_count, $jobs, "$jobs jobs done");
    ok($lost_race, "lost the race at least once");
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
    while (my $job = Worker::Addition->grab_job($client)) {
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
sub grab_for { 5 }

