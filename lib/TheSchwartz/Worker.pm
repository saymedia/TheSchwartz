# $Id$

package TheSchwartz::Worker;
use strict;

use Carp qw( croak );
use Storable ();

sub grab_job {
    my $class = shift;
    my($client) = @_;
    return scalar $client->lookup_jobs_by_function($class->handles);
}

sub handles {
    return $_[0];
}

sub keep_exit_status_for { 0 }
sub max_retries { 0 }
sub retry_delay { 0 }

sub work_safely {
    my $worker = shift;
    my($job) = @_;
    my $res;
    eval {
        $res = $worker->work($job);
    };
    if ($@) {
        $job->add_failure($@);
    }
    return $res;
}

1;
