# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 8;

run_tests(4, sub {
    my $client = test_client(dbs      => ['ts1', 'ts2']);

    my $n_jobs = 60;
    for (1..$n_jobs) {
        my $handle = $client->insert("Worker::Foo");
        die unless $handle;
    }

    my $db1 = DBI->connect(dsn_for("ts1"), 'root', '');
    my $db2 = DBI->connect(dsn_for("ts2"), 'root', '');
    die unless $db1 && $db2;

    my $jobs1 = $db1->selectrow_array("SELECT COUNT(*) FROM job");
    my $jobs2 = $db2->selectrow_array("SELECT COUNT(*) FROM job");
    is($jobs1 + $jobs2, $n_jobs, "inserted all $n_jobs");

    ok($jobs1 > $n_jobs / 4, "at least a quarter of jobs went to db1 ($jobs1 / $n_jobs)");
    ok($jobs2 > $n_jobs / 4, "at least a quarter of jobs went to db1 ($jobs2 / $n_jobs)");

    my $do_jobs = int($n_jobs / 2);
    $client->can_do("Worker::Foo");
    for (1..$do_jobs) {
        $client->work_once
            or die;
    }

    my $jobs1b = $db1->selectrow_array("SELECT COUNT(*) FROM job");
    my $jobs2b = $db2->selectrow_array("SELECT COUNT(*) FROM job");

    my $remain_jobs = $n_jobs - $do_jobs;
    is($jobs1b + $jobs2b, $remain_jobs, "expected jobs remain");

    # deltas: how much work gone done each
    my $jobs1d = $jobs1 - $jobs1b;
    my $jobs2d = $jobs2 - $jobs2b;

    # difference in work done:
    my $workdiff = abs($jobs1d - $jobs2d);

});

sub max { $_[0] > $_[1] ? $_[0] : $_[1] }

package Worker::Foo;
use base 'TheSchwartz::Worker';
sub work {
    my ($class, $job) = @_;
    $job->completed;
}
