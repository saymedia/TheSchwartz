# $Id$

package TheSchwartz;
use strict;
use fields qw( databases abilities );

use Carp qw( croak );
use Data::ObjectDriver::Driver::DBI;
use Digest::MD5 qw( md5_hex );
use TheSchwartz::Job;
use TheSchwartz::JobHandle;

sub new {
    my TheSchwartz $client = shift;
    my %args = @_;
    $client = fields::new($client) unless ref $client;

    croak "databases must be an arrayref if specified"
        unless !exists $args{databases} || ref $args{databases} eq 'ARRAY';
    my $databases = delete $args{databases};

    croak "unknown options ", join(', ', keys %args) if keys %args;

    $client->hash_databases($databases);
    $client->reset_abilities;

    return $client;
}

sub hash_databases {
    my $client = shift;
    my($list) = @_;
    for my $ref (@$list) {
        my $full = join '|', map { $ref->{$_} } qw( dsn user pass );
        $client->{databases}{ md5_hex($full) } = $ref;
    }
}

sub driver_for {
    my $client = shift;
    my($hashdsn) = @_;
    my $db = $client->{databases}{$hashdsn};
    return Data::ObjectDriver::Driver::DBI->new(
            dsn      => $db->{dsn},
            username => $db->{user},
            password => $db->{pass},
        );
}

sub lookup_job {
    my $client = shift;
    my $handle = $client->handle_from_string(@_);
    my $driver = $client->driver_for($handle->dsn_hashed);
    my $job = $driver->lookup('TheSchwartz::Job' => $handle->jobid)
        or return;
    $job->handle($handle);
    return $job;
}

sub find_job_for_workers {
    my $client = shift;
    my($worker_classes) = @_;
    my %functions = map { $_->handles => $_ } @$worker_classes;

    for my $hashdsn (keys %{ $client->{databases} }) {
        my $driver = $client->driver_for($hashdsn);

        ## Start a transaction, because if we find a job we'll need to
        ## update it.
        $driver->begin_work;

        ## Search for jobs in this database where:
        ## 1. funcname is in the list of abilities this $client supports;
        ## 2. the job is scheduled to be run (run_after is in the past);
        ## 3. no one else is working on the job (grabbed_until is NULL or
        ##    in the past).
        my($job) = $driver->search('TheSchwartz::Job' => {
                funcname      => [ keys %functions ],
                run_after     => { op => '<=', value => time },
                grabbed_until => [
                    \'IS NULL',
                    { op => '<=', value => time },
                ],
            });

        if ($job) {
            ## Got a job!
            ## Update the job's grabbed_until column so that
            ## no one else takes it.
            my $worker_class = $functions{$job->funcname};
            $job->grabbed_until(time + $worker_class->grab_for);

            ## Update the job in the database, and end the transaction.
            $driver->update($job);
            $driver->commit;

            ## Now prepare the job, and return it.
            my $handle = TheSchwartz::JobHandle->new({
                    dsn_hashed => $hashdsn,
                    jobid      => $job->jobid,
                });
            $handle->client($client);
            $job->handle($handle);
            return $job;
        }

        ## If we didn't find a job, we need to commit to end the
        ## transaction in this database.
        $driver->commit;
    }
}

sub choose_database {
    my $client = shift;
    my @dsns = keys %{ $client->{databases} };
    $dsns[rand @dsns];
}

sub insert {
    my $client = shift;
    my($job) = @_;
    unless (ref($job) eq 'TheSchwartz::Job') {
        $job = TheSchwartz::Job->new_from_array($job, $_[1]);
    }
    my $hashdsn = $client->choose_database;
    my $driver = $client->driver_for($hashdsn);
    $driver->insert($job);

    my $handle = TheSchwartz::JobHandle->new({
            dsn_hashed => $hashdsn,
            client     => $client,
            jobid      => $job->jobid
        });
    $job->handle($handle);

    return $handle;
}

sub handle_from_string {
    my $client = shift;
    my $handle = TheSchwartz::JobHandle->new_from_string(@_);
    $handle->client($client);
    return $handle;
}

sub can_do {
    my $client = shift;
    my($key, $class) = @_;
    $class ||= $key;
    $client->{abilities}{$key} = $class;
}

sub reset_abilities {
    my $client = shift;
    $client->{abilities} = {};
}

sub work_until_done {
    my $client = shift;
    while (1) {
        my $job = $client->find_job_for_workers([
                values %{ $client->{abilities} }
            ]);
        last unless $job;

        my $class = $client->{abilities}{ $job->funcname };
        $class->work_safely($job);
    }
}

1;
