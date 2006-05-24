# $Id$

package TheSchwartz;
use strict;
use fields qw( databases retry_seconds dead_dsns retry_at
               funcmap_cache verbose
               all_abilities current_abilities );

use Carp qw( croak );
use Data::ObjectDriver::Errors;
use Data::ObjectDriver::Driver::DBI;
use Digest::MD5 qw( md5_hex );
use List::Util qw( shuffle );
use TheSchwartz::FuncMap;
use TheSchwartz::Job;
use TheSchwartz::JobHandle;

use constant RETRY_DEFAULT => 30;
use constant OK_ERRORS => { map { $_ => 1 }
    Data::ObjectDriver::Errors->UNIQUE_CONSTRAINT,
};

sub new {
    my TheSchwartz $client = shift;
    my %args = @_;
    $client = fields::new($client) unless ref $client;

    croak "databases must be an arrayref if specified"
        unless !exists $args{databases} || ref $args{databases} eq 'ARRAY';
    my $databases = delete $args{databases};

    $client->{retry_seconds} = delete $args{retry_seconds} || RETRY_DEFAULT;
    $client->set_verbose(delete $args{verbose});

    croak "unknown options ", join(', ', keys %args) if keys %args;

    $client->hash_databases($databases);
    $client->reset_abilities;
    $client->{dead_dsns} = {};
    $client->{retry_at} = {};
    $client->{funcmap_cache} = {};

    return $client;
}

sub debug {
    my TheSchwartz $client = shift;
    return unless $client->{verbose};
    $client->{verbose}->($_[0]);
}

sub hash_databases {
    my TheSchwartz $client = shift;
    my($list) = @_;
    for my $ref (@$list) {
        my $full = join '|', map { $ref->{$_} } qw( dsn user pass );
        $client->{databases}{ md5_hex($full) } = $ref;
    }
}

sub driver_for {
    my TheSchwartz $client = shift;
    my($hashdsn) = @_;
    my $db = $client->{databases}{$hashdsn};
    return Data::ObjectDriver::Driver::DBI->new(
            dsn      => $db->{dsn},
            username => $db->{user},
            password => $db->{pass},
            ($db->{prefix} ? (prefix   => $db->{prefix}) : ()),
        );
}

sub mark_database_as_dead {
    my TheSchwartz $client = shift;
    my($hashdsn) = @_;
    $client->{dead_dsns}{$hashdsn} = 1;
    $client->{retry_at}{$hashdsn} = time + $client->{retry_seconds};
}

sub is_database_dead {
    my TheSchwartz $client = shift;
    my($hashdsn) = @_;
    ## If this database is marked as dead, check the retry time. If
    ## it has passed, try the database again to see if it's undead.
    if ($client->{dead_dsns}{$hashdsn}) {
        if ($client->{retry_at}{$hashdsn} < time) {
            delete $client->{dead_dsns}{$hashdsn};
            delete $client->{retry_at}{$hashdsn};
            return 0;
        } else {
            return 1;
        }
    }
    return 0;
}

sub lookup_job {
    my TheSchwartz $client = shift;
    my $handle = $client->handle_from_string(@_);
    my $driver = $client->driver_for($handle->dsn_hashed);
    my $job = $driver->lookup('TheSchwartz::Job' => $handle->jobid)
        or return;
    $job->handle($handle);
    $job->funcname( $client->funcid_to_name($driver, $handle->dsn_hashed, $job->funcid) );
    return $job;
}

sub find_job_for_workers {
    my TheSchwartz $client = shift;
    my($worker_classes) = @_;
    $worker_classes ||= $client->{current_abilities};

    for my $hashdsn ($client->shuffled_databases) {
        ## If the database is dead, skip it.
        next if $client->is_database_dead($hashdsn);

        my $driver = $client->driver_for($hashdsn);

        my $job;
        eval {
            ## Start a transaction, because if we find a job we'll need to
            ## update it.
            $driver->begin_work;

            ## Search for jobs in this database where:
            ## 1. funcname is in the list of abilities this $client supports;
            ## 2. the job is scheduled to be run (run_after is in the past);
            ## 3. no one else is working on the job (grabbed_until is NULL or
            ##    in the past).
            my @ids = map { $client->funcname_to_id($driver, $hashdsn, $_) }
                      @$worker_classes;
            ($job) = $driver->search('TheSchwartz::Job' => {
                    funcid        => \@ids,
                    run_after     => { op => '<=', value => time },
                    grabbed_until => [
                        \'IS NULL',
                        { op => '<=', value => time },
                    ],
                }, { limit => 1 });
        };
        if ($@) {
            unless (OK_ERRORS->{ $driver->last_error }) {
                $client->mark_database_as_dead($hashdsn);
            }
        }

        if ($job) {
            ## Got a job!

            ## Convert the funcid to a funcname, based on this database's map.
            $job->funcname( $client->funcid_to_name($driver, $hashdsn, $job->funcid) );

            ## Update the job's grabbed_until column so that
            ## no one else takes it.
            my $worker_class = $job->funcname;
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

sub shuffled_databases {
    my TheSchwartz $client = shift;
    my @dsns = keys %{ $client->{databases} };
    return shuffle(@dsns);
}

sub insert_job_to_driver {
    my $client = shift;
    my($job, $driver, $hashdsn) = @_;
    eval {
        ## Set the funcid of the job, based on the funcname. Since each
        ## database has a separate cache, this needs to be calculated based
        ## on the hashed DSN. Also: this might fail, if the database is dead.
        $job->funcid( $client->funcname_to_id($driver, $hashdsn, $job->funcname) );

        ## Now, insert the job. This also might fail.
        $driver->insert($job);
    };
    if ($@) {
        unless (OK_ERRORS->{ $driver->last_error }) {
            $client->mark_database_as_dead($hashdsn);
        }
    } elsif ($job->jobid) {
        ## We inserted the job successfully!
        ## Attach a handle to the job, and return the handle.
        my $handle = TheSchwartz::JobHandle->new({
                dsn_hashed => $hashdsn,
                client     => $client,
                jobid      => $job->jobid
            });
        $job->handle($handle);
        return $handle;
    }
    return undef;
}

sub insert {
    my TheSchwartz $client = shift;
    my($job) = @_;
    unless (ref($job) eq 'TheSchwartz::Job') {
        $job = TheSchwartz::Job->new_from_array($job, $_[1]);
    }

    ## Try each of the databases that are registered with $client, in
    ## random order. If we successfully create the job, exit the loop.
    my $dead = $client->{dead_dsns};
    my $retry = $client->{retry_at};
    for my $hashdsn ($client->shuffled_databases) {
        ## If the database is dead, skip it.
        next if $client->is_database_dead($hashdsn);

        my $driver = $client->driver_for($hashdsn);

        ## Try to insert the job into this database. If we get a handle
        ## back, return it.
        my $handle = $client->insert_job_to_driver($job, $driver, $hashdsn);
        return $handle if $handle;
    }

    ## If the job wasn't submitted successfully to any database, return.
    return undef;
}

sub handle_from_string {
    my TheSchwartz $client = shift;
    my $handle = TheSchwartz::JobHandle->new_from_string(@_);
    $handle->client($client);
    return $handle;
}

sub can_do {
    my TheSchwartz $client = shift;
    my($class) = @_;
    push @{ $client->{all_abilities} }, $class;
    push @{ $client->{current_abilities} }, $class;
}

sub reset_abilities {
    my TheSchwartz $client = shift;
    $client->{all_abilities} = [];
    $client->{current_abilities} = [];
}

sub restore_full_abilities {
    my $client = shift;
    $client->{current_abilities} = [ @{ $client->{all_abilities} } ];
}

sub temporarily_remove_ability {
    my $client = shift;
    my($class) = @_;
    $client->{current_abilities} = [
            grep { $_ ne $class } @{ $client->{current_abilities} }
        ];
    if (!@{ $client->{current_abilities} }) {
        $client->restore_full_abilities;
    }
}

sub work {
    my TheSchwartz $client = shift;
    my($delay) = @_;
    $delay ||= 5;
    while (1) {
        sleep $delay unless $client->work_once;
    }
}

sub work_until_done {
    my TheSchwartz $client = shift;
    while (1) {
        $client->work_once or last;
    }
}

## Returns true if it did something, false if no jobs were found
sub work_once {
    my TheSchwartz $client = shift;

    ## Look for a job with our current set of abilities. Note that the
    ## list of current abilities may not be equal to the full set of
    ## abilities, to allow for even distribution between jobs.
    my $job = $client->find_job_for_workers;

    ## If we didn't find anything, restore our full abilities, and try
    ## again.
    if (!$job &&
        @{ $client->{current_abilities} } < @{ $client->{all_abilities} }) {
        $client->restore_full_abilities;
        $job = $client->find_job_for_workers;
    }

    my $class = $job ? $job->funcname : undef;
    if ($job) {
        $client->debug("TheSchwartz::work_once got job of class '$class'");
    } else {
        $client->debug("TheSchwartz::work_once found no jobs");
    }

    ## If we still don't have anything, return.
    return unless $job;

    ## Now that we found a job for this particular funcname, remove it
    ## from our list of current abilities. So the next time we look for a
    ## we'll find a job for a different funcname. This prevents starvation of
    ## high funcid values because of the way MySQL's indexes work.
    $client->temporarily_remove_ability($class);

    $class->work_safely($job);

    ## We got a job, so return 1 so work_until_done (which calls this method)
    ## knows to keep looking for jobs.
    return 1;
}

sub funcid_to_name {
    my TheSchwartz $client = shift;
    my($driver, $hashdsn, $funcid) = @_;
    my $cache = $client->_funcmap_cache($hashdsn);
    return $cache->{funcid2name}{$funcid};
}

sub funcname_to_id {
    my TheSchwartz $client = shift;
    my($driver, $hashdsn, $funcname) = @_;
    my $cache = $client->_funcmap_cache($hashdsn);
    unless (exists $cache->{funcname2id}{$funcname}) {
        my $map = TheSchwartz::FuncMap->create_or_find($driver, $funcname);
        $cache->{funcname2id}{ $map->funcname } = $map->funcid;
        $cache->{funcid2name}{ $map->funcid }   = $map->funcname;
    }
    return $cache->{funcname2id}{$funcname};
}

sub _funcmap_cache {
    my TheSchwartz $client = shift;
    my($hashdsn) = @_;
    unless (exists $client->{funcmap_cache}{$hashdsn}) {
        my $driver = $client->driver_for($hashdsn);
        my @maps = $driver->search('TheSchwartz::FuncMap');
        my $cache = { funcname2id => {}, funcid2name => {} };
        for my $map (@maps) {
            $cache->{funcname2id}{ $map->funcname } = $map->funcid;
            $cache->{funcid2name}{ $map->funcid }   = $map->funcname;
        }
        $client->{funcmap_cache}{$hashdsn} = $cache;
    }
    return $client->{funcmap_cache}{$hashdsn};
}

# accessors

sub verbose {
    my TheSchwartz $client = shift;
    return $client->{verbose};
}

sub set_verbose {
    my TheSchwartz $client = shift;
    my $logger = shift;   # or non-coderef to just print to stderr
    if ($logger && ref $logger ne "CODE") {
        $logger = sub {
            my $msg = shift;
            $msg =~ s/\s+$//;
            print STDERR "$msg\n";
        };
    }
    $client->{verbose} = $logger;
}

1;
