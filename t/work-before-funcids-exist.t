# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 4;

run_tests(2, sub {
    my $client = test_client(dbs => ['ts1']);

    my $handle = $client->insert("Worker::Dummy");
    ok($handle, "inserted job");

    $client->can_do("Worker::Dummy");
    $client->can_do("Worker::Dummy2");
    $client->can_do("Worker::Dummy3");
    $client->work_until_done;

    ok(! $handle->is_pending, "job is done");
});



############################################################################
package Worker::Dummy;
use base 'TheSchwartz::Worker';
sub work {
    my ($class, $job) = @_;
    my $subjob = TheSchwartz::Job->new(
                                       funcname => 'Worker::Dummy2',
                                       );
    $job->replace_with($subjob);
}

sub max_retries { 2 }
sub retry_delay { 5 }



package Worker::Dummy2;
use base 'TheSchwartz::Worker';
sub work {
    my ($class, $job) = @_;
    $job->completed;
}

package Worker::Dummy3;
use base 'TheSchwartz::Worker';
sub work {
    my ($class, $job) = @_;
    $job->completed;
}



