# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 8;

run_tests(4, sub {
    my $client = test_client(dbs      => ['ts1']);

    my $n_jobs = 10;
    for (1..$n_jobs) {
        $client->insert("Worker::Job1") or die;
        $client->insert("Worker::Job2") or die;
    }

    my $db1 = DBI->connect(dsn_for("ts1"), 'root', '');
    die unless $db1;

    my $jobs1 = $db1->selectrow_array("SELECT COUNT(*) FROM job WHERE funcid=1");
    is($jobs1, $n_jobs, "have $n_jobs funcid 1s");
    my $jobs2 = $db1->selectrow_array("SELECT COUNT(*) FROM job WHERE funcid=2");
    is($jobs2, $n_jobs, "have $n_jobs funcid 2s");

    my $do_jobs = int($n_jobs / 2);
    $client->can_do("Worker::Job1");
    $client->can_do("Worker::Job2");
    for (1..($do_jobs * 2)) {
        $client->work_once
            or die "Couldn't find job to do";
    }

    my $jobs1b = $db1->selectrow_array("SELECT COUNT(*) FROM job WHERE funcid=1");
    is($jobs1b, $n_jobs - $do_jobs, "have half funcid 1s");
    my $jobs2b = $db1->selectrow_array("SELECT COUNT(*) FROM job WHERE funcid=2");
    is($jobs2b, $n_jobs - $do_jobs, "have half funcid 2s");


});

package Worker::Job1;
use base 'TheSchwartz::Worker';
sub work {
    my ($class, $job) = @_;
    $job->completed;
}

package Worker::Job2;
use base 'Worker::Job1';

