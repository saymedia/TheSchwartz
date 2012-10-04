# -*-perl-*-

# Special test for pg
# pg dont have replace sql syntax, so a duplicate job will croak and
# bring the dbh in a invalid state

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 15;

run_tests_pgsql(13, sub {
    my $client1 = test_client(dbs => ['ts1']);
    my $client2 = test_client(dbs => ['ts1']);

    my $driver = $client1->driver_for( ($client1->shuffled_databases)[0] );
    my $dbh = $driver->rw_handle;

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey IN ('1','2','3','4','5');"),
        0,
        'namespace empty',
    );


    $client1->can_do('Test::Job::Completed');
    $client2->can_do('Test::Job::Replace');

# job 1
    $client1->insert(TheSchwartz::Job->new(
        funcname => 'Test::Job::Completed',
        uniqkey  => 1,
    ));

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '1';"),
        1,
        'Job 1 posted',
    );


# Job 1 
    $client1->work_once;

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '1';"),
        0,
        'Job 1 done',
    );

# Job 2
    $client2->insert(TheSchwartz::Job->new(
        funcname => 'Test::Job::Replace',
        uniqkey  => 2,
        arg      => 3,
    ));

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '2';"),
        1,
        'Job 2 posted',
    );

# Job 2
    $client2->work_once;

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '2';"),
        0,
        'Job 2 done',
    );
    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '3';"),
        1,
        'Job 2 replaced with Job 3',
    );

# Job 4
    $client2->insert(TheSchwartz::Job->new(
        funcname => 'Test::Job::Replace',
        uniqkey  => 4,
        arg      => 3,
    ));

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '4';"),
        1,
        'Job 4 posted',
    );

# Job 4
    $client2->work_once;

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM error LEFT JOIN job USING(jobid) WHERE uniqkey = '4';"),
        1,
        'Job 4 throw error',
    );
    like(
        $dbh->selectrow_array("SELECT message FROM error LEFT JOIN job USING(jobid) WHERE uniqkey = '4';"),
        qr/job_funcid_uniqkey/,
        'Job 4 throw uniqkey',
    );
    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '4';"),
        1,
        'Job 4 canceled',
    );
    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '3';"),
        1,
        'Job 4 not replaced with Job 3',
    );

# Job 3
    $client1->work_once;

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '3';"),
        0,
        'Job 3 done',
    );

# cleanup job.run_after & retry_at, so we dont have to wait
    $dbh->do("UPDATE job SET run_after = 0 WHERE uniqkey = '4';");
    $client2->{retry_at} = {};

# Job 4
    $client2->work_once;


    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '4';"),
        0,
        'Job 4 done',
    );
    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '3';"),
        1,
        'Job 4 replaced with Job 3',
    );

# Job 5
    $client1->work_once;

    is(
        $dbh->selectrow_array("SELECT COUNT(*) FROM job WHERE uniqkey = '3';"),
        0,
        'Job 3 done - again',
    );
});




# TheSchwartz Worker/Jobs
package Test::Job::Completed;

use base qw(TheSchwartz::Worker);

sub work {
    my ($client, $job) = @_;
    $job->completed;
}
sub max_retries { 10; }

package Test::Job::Replace;

use base qw(TheSchwartz::Worker);

sub work {
    my ($client, $job) = @_;
    $job->replace_with(TheSchwartz::Job->new(
        funcname => 'Test::Job::Completed',
        uniqkey  => $job->arg,
    ));
}
sub max_retries { 10; }

