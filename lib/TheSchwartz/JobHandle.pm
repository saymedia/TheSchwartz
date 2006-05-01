# $Id$

package TheSchwartz::JobHandle;
use strict;
use base qw( Class::Accessor::Fast );

__PACKAGE__->mk_accessors(qw( dsn_hashed jobid client ));

use TheSchwartz::Error;
use TheSchwartz::Job;

sub new_from_string {
    my $class = shift;
    my($hstr) = @_;
    my($hashdsn, $jobid) = split /\-/, $hstr, 2;
    return TheSchwartz::JobHandle->new({
            dsn_hashed => $hashdsn,
            jobid      => $jobid,
        });
}

sub as_string {
    my $handle = shift;
    return join '-', $handle->dsn_hashed, $handle->jobid;
}

sub job {
    my $handle = shift;
    my $job = $handle->client->lookup_job($handle->as_string);
    $job->handle($handle);
    return $job;
}

sub status {
}

sub is_pending { 'pending' }

sub exit_status { }

sub failure_log {
    my $handle = shift;
    my $driver = $handle->client->driver_for($handle->dsn_hashed);
    my @failures = $driver->search('TheSchwartz::Error' =>
            { jobid => $handle->jobid },
        );
    return map { $_->message } @failures;
}

sub failures {
    my $handle = shift;
    return scalar $handle->failure_log;
}

1;
