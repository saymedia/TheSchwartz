# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 30;

# for testing:
$TheSchwartz::T_EXITSTATUS_CLEAN_THRES = 1;  # delete 100% of the time, not 10% of the time
$TheSchwartz::T_ERRORS_MAX_AGE = 2;          # keep errors for 3 seconds, not 1 week

run_tests(10, sub {
    my $client = test_client(dbs => ['ts1']);
    my $dbh = DBI->connect(dsn_for("ts1"), $ENV{TS_DB_USER}, $ENV{TS_DB_PASS});
    $client->can_do("Worker::Fail");
    $client->can_do("Worker::Complete");

    # insert a job which will fail, then succeed.
    {
        my $handle = $client->insert("Worker::Fail");
        isa_ok $handle, 'TheSchwartz::JobHandle', "inserted job";

        $client->work_until_done;
        is($handle->failures, 1, "job has failed once");

        my $min;
        my $rows = $dbh->selectrow_array("SELECT COUNT(*) FROM exitstatus");
        is($rows, 1, "has 1 exitstatus row");

        ok($client->insert("Worker::Complete"), "inserting to-pass job");
        $client->work_until_done;
        $rows = $dbh->selectrow_array("SELECT COUNT(*) FROM exitstatus");
        is($rows, 2, "has 2 exitstatus rows");
        ($rows, $min) = $dbh->selectrow_array("SELECT COUNT(*), MIN(jobid) FROM error");
        is($rows, 1, "has 1 error rows");
        is($min, 1, "error jobid is the old one");

        # wait for exit status to pass
        sleep 3;

        # now make another job fail to cleanup some errors
        $handle = $client->insert("Worker::Fail");
        $client->work_until_done;

        $rows = $dbh->selectrow_array("SELECT COUNT(*) FROM exitstatus");
        is($rows, 1, "1 exit status row now");

        ($rows, $min) = $dbh->selectrow_array("SELECT COUNT(*), MIN(jobid) FROM error");
        is($rows, 1, "has 1 error row still");
        is($min, 3, "error jobid is only the new one");

    }

    teardown_dbs('ts1');
});

############################################################################
package Worker::Fail;
use base 'TheSchwartz::Worker';

sub work {
    my ($class, $job) = @_;
    $job->failed("an error message");
    return;
}

sub keep_exit_status_for { 1 }  # keep exit status for 20 seconds after on_complete

sub max_retries { 0 }

sub retry_delay { 1 }

# ---------------

package Worker::Complete;
use base 'TheSchwartz::Worker';
sub work {
    my ($class, $job) = @_;
    $job->completed;
    return;
}

sub keep_exit_status_for { 1 }

