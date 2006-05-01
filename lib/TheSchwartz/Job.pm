# $Id$

package TheSchwartz::Job;
use strict;
use base qw( Data::ObjectDriver::BaseObject );

use Carp qw( croak );
use Storable ();
use TheSchwartz::Error;
use TheSchwartz::ExitStatus;
use TheSchwartz::JobHandle;

__PACKAGE__->install_properties({
               columns     => [qw(jobid funcname arg uniqkey insert_time
                                  run_after grabbed_until priority coalesce)],
               datasource  => 'job',
               primary_key => 'jobid',
           });

__PACKAGE__->add_trigger(pre_save => sub {
    my($job) = @_;
    if (my $arg = $job->arg) {
        $job->arg(Storable::nfreeze($arg));
    }
});

__PACKAGE__->add_trigger(post_load => sub {
    my($job) = @_;
    if (my $arg = $job->arg) {
        $job->arg(Storable::thaw($arg));
    }
});

sub new_from_array {
    my $class = shift;
    my(@arg) = @_;
    croak "usage: new_from_array(funcname, arg)" unless @arg == 2;
    return $class->new(
            funcname => $arg[0],
            arg      => $arg[1],
        );
}

sub new {
    my $class = shift;
    my(%param) = @_;
    my $job = $class->SUPER::new;
    if (my $arg = $param{arg}) {
        if (ref($arg) eq 'SCALAR') {
            $param{arg} = Storable::thaw($$arg);
        } elsif (!ref($arg)) {
            $param{arg} = Storable::thaw($arg);
        }
    }
    for my $key (keys %param) {
        $job->$key($param{$key});
    }
    return $job;
}

sub handle {
    my $job = shift;
    if (@_) {
        $job->{__handle} = $_[0];
    }
    return $job->{__handle};
}

sub driver {
    my $job = shift;
    unless (exists $job->{__driver}) {
        my $handle = $job->handle;
        $job->{__driver} = $handle->client->driver_for($handle->dsn_hashed);
    }
    return $job->{__driver};
}

sub add_failure {
    my $job = shift;
    my($msg) = @_;
    my $error = TheSchwartz::Error->new;
    $error->jobid($job->jobid);
    $error->message($msg);
    $job->driver->insert($error);
    return $error;
}

sub set_exit_status {
    my $job = shift;
    my($exit) = @_;
    my $status = TheSchwartz::ExitStatus->new;
    $status->jobid($job->jobid);
    $status->status($exit);
    $job->driver->insert($status);
    return $status;
}

sub completed {
    my $job = shift;
    $job->set_exit_status(0);
    $job->driver->remove($job);
}

sub failed {
    my $job = shift;
}

sub replace_with {
    my $job = shift;
    my(@jobs) = @_;

    ## The new jobs @jobs should be inserted into the same database as $job,
    ## which they're replacing. So get a driver for the database that $job
    ## belongs to.
    my $driver = $job->driver;

    ## Start a transaction.
    $driver->begin_work;

    ## Insert the new jobs.
    for my $j (@jobs) {
        $driver->insert($j);
    }

    ## Mark the original job as completed successfully.
    $job->completed;

    ## Looks like it's all ok, so commit.
    $driver->commit;
}

1;
