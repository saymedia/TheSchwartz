# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 4;

run_tests(2, sub {
    setup_dbs('ts1');
    teardown_dbs('ts2');  # doesn't exist

    my $client = test_client(dbs => ['ts2', 'ts1'],
                             init => 0);

    # insert a job
    my $n_handles = 0;
    for (1..50) {
        my $handle = $client->insert("Worker::Addition", { numbers => [1, 2] });
        $n_handles++ if $handle;
    }
    is($n_handles, 50, "got 50 handles");

    # let's do some work.  the tedious way, specifying which class should grab a job
    my $n_grabbed = 0;
    while (my $job = Worker::Addition->grab_job($client)) {
        $n_grabbed++;
    }
    is($n_grabbed, 50, "grabbed 50 times");

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

