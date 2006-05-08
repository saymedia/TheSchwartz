# $Id$

package TheSchwartz::Worker;
use strict;

use Carp qw( croak );
use Storable ();

sub grab_job {
    my $class = shift;
    my($client) = @_;
    return $client->find_job_for_workers([ $class ]);
}

sub handles {
    return $_[0];
}

sub keep_exit_status_for { 0 }
sub max_retries { 0 }
sub retry_delay { 0 }
sub grab_for { 60 * 60 }   ## 1 hour

sub work_safely {
    my $worker = shift;
    my($job) = @_;
    my $res;
    eval {
        $res = $worker->work($job);
    };
    if ($@) {
        $job->failed($@);
    }
    unless ($job->did_something) {
        $job->failed('Job did not explicitly complete, fail, or get replaced');
    }
    return $res;
}

1;