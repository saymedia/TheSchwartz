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
               columns     => [qw(jobid funcid arg uniqkey insert_time
                                  run_after grabbed_until priority coalesce)],
               datasource  => 'job',
               column_defs => { arg => 'blob' },
               primary_key => 'jobid',
           });

__PACKAGE__->add_trigger(pre_save => sub {
    my ($job) = @_;
    my $arg = $job->arg
        or return;
    if (ref($arg)) {
        $job->arg(Storable::nfreeze($arg));
    }
});

__PACKAGE__->add_trigger(post_load => sub {
    my ($job) = @_;
    my $arg = $job->arg
        or return;

    my $magic = eval { Storable::read_magic($arg); };
    if ($magic && $magic->{major} && $magic->{major} >= 2) {
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
            # if a regular scalar, test to see if it's a storable or not.
            my $magic = eval { Storable::read_magic($arg); };
            if ($magic && $magic->{major} && $magic->{major} >= 2) {
                $param{arg} = Storable::thaw($arg);
            } else {
                $param{arg} = $arg;
            }
        }
    }
    $param{run_after} ||= time;
    $param{grabbed_until} ||= 0;
    for my $key (keys %param) {
        $job->$key($param{$key});
    }
    return $job;
}

sub funcname {
    my $job = shift;
    if (@_) {
        $job->{__funcname} = shift;
    }

    # lazily load,
    if (!$job->{__funcname}) {
        my $handle = $job->handle;
        my $client = $handle->client;
        my $driver = $client->driver_for($handle->dsn_hashed);
        my $funcname = $client->funcid_to_name($driver, $handle->dsn_hashed, $job->funcid)
            or die "Failed to lookup funcname of job $job";
        return $job->{__funcname} = $funcname;
    }
    return $job->{__funcname};
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
    $error->error_time(time());
    $error->jobid($job->jobid);
    $error->funcid($job->funcid);
    $error->message($msg || '');

    my $driver = $job->driver;
    $driver->insert($error);

    # and let's lazily clean some errors while we're here.
    my $unixtime = $driver->dbd->sql_for_unixtime;
    my $maxage   = $TheSchwartz::T_ERRORS_MAX_AGE || (86400*7);
    $driver->remove('TheSchwartz::Error', {
        error_time => \ "< $unixtime - $maxage",
    }, {
        nofetch => 1,
        limit   => $driver->dbd->can_delete_with_limit ? 1000 : undef,
    });

    return $error;
}

sub exit_status { shift->handle->exit_status }
sub failure_log { shift->handle->failure_log }
sub failures    { shift->handle->failures    }

sub set_exit_status {
    my $job = shift;
    my($exit) = @_;
    my $class = $job->funcname;
    my $secs = $class->keep_exit_status_for or return;
    my $status = TheSchwartz::ExitStatus->new;
    $status->jobid($job->jobid);
    $status->completion_time(time);
    $status->delete_after($status->completion_time + $secs);
    $status->status($exit);

    my $driver = $job->driver;
    $driver->insert($status);

    # and let's lazily clean some exitstatus while we're here.  but
    # rather than doing this query all the time, we do it 1/nth of the
    # time, and deleting up to n*10 queries while we're at it.
    # default n is 10% of the time, doing 100 deletes.
    my $clean_thres = $TheSchwartz::T_EXITSTATUS_CLEAN_THRES || 0.10;
    if (rand() < $clean_thres) {
        my $unixtime = $driver->dbd->sql_for_unixtime;
        $driver->remove('TheSchwartz::ExitStatus', {
            delete_after => \ "< $unixtime",
        }, {
            nofetch => 1,
            limit   => $driver->dbd->can_delete_with_limit ? int(1 / $clean_thres * 100) : undef,
        });
    }

    return $status;
}

sub did_something {
    my $job = shift;
    if (@_) {
        $job->{__did_something} = shift;
    }
    return $job->{__did_something};
}

sub debug {
    my ($job, $msg) = @_;
    $job->handle->client->debug($msg, $job);
}

sub completed {
    my $job = shift;
    $job->debug("job completed");
    if ($job->did_something) {
        $job->debug("can't call 'completed' on already finished job");
        return 0;
    }
    $job->did_something(1);
    $job->set_exit_status(0);
    $job->driver->remove($job);
}

sub permanent_failure {
    my ($job, $msg, $ex_status) = @_;
    if ($job->did_something) {
        $job->debug("can't call 'permanent_failure' on already finished job");
        return 0;
    }
    $job->_failed($msg, $ex_status, 0);
}

sub failed {
    my ($job, $msg, $ex_status) = @_;
    if ($job->did_something) {
        $job->debug("can't call 'failed' on already finished job");
        return 0;
    }

    ## If this job class specifies that jobs should be retried,
    ## update the run_after if necessary, but keep the job around.

    my $class       = $job->funcname;
    my $failures    = $job->failures + 1;    # include this one, since we haven't ->add_failure yet
    my $max_retries = $class->max_retries($job);

    $job->debug("job failed.  considering retry.  is max_retries of $max_retries >= failures of $failures?");
    $job->_failed($msg, $ex_status, $max_retries >= $failures, $failures);
}

sub _failed {
    my ($job, $msg, $exit_status, $_retry, $failures) = @_;
    $job->did_something(1);
    $job->debug("job failed: " . ($msg || "<no message>"));

    ## Mark the failure in the error table.
    $job->add_failure($msg);

    if ($_retry) {
        my $class = $job->funcname;
        if (my $delay = $class->retry_delay($failures)) {
            $job->run_after(time() + $delay);
        }
        $job->grabbed_until(0);
        $job->driver->update($job);
    } else {
        $job->set_exit_status($exit_status || 1);
        $job->driver->remove($job);
    }
}

sub replace_with {
    my $job = shift;
    my(@jobs) = @_;

    if ($job->did_something) {
        $job->debug("can't call 'replace_with' on already finished job");
        return 0;
    }
    # Note: we don't set 'did_something' here because completed does it down below.

    ## The new jobs @jobs should be inserted into the same database as $job,
    ## which they're replacing. So get a driver for the database that $job
    ## belongs to.
    my $handle = $job->handle;
    my $client = $handle->client;
    my $hashdsn = $handle->dsn_hashed;
    my $driver = $job->driver;

    $job->debug("replacing job with " . (scalar @jobs) . " other jobs");

    ## Start a transaction.
    $driver->begin_work;

    ## Insert the new jobs.
    for my $j (@jobs) {
        $client->insert_job_to_driver($j, $driver, $hashdsn);
    }

    ## Mark the original job as completed successfully.
    $job->completed;

    # for testing
    if ($TheSchwartz::Job::_T_REPLACE_WITH_FAIL) {
        $driver->rollback;
        die "commit failed for driver: due to testing\n";
    }

    ## Looks like it's all ok, so commit.
    $driver->commit;
}

sub set_as_current {
    my $job = shift;
    my $client = $job->handle->client;
    $client->set_current_job($job);
}

1;
