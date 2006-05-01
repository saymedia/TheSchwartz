# $Id$

package TheSchwartz;
use strict;
use fields qw( databases hash2dsn );

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
    return $driver->lookup('TheSchwartz::Job' => $handle->jobid);
}

sub insert {
    my $client = shift;
    my $job = shift;
    unless (ref($job) eq 'TheSchwartz::Job') {
        my %arg = @_;
        $job = TheSchwartz::Job->new_from_array($job, \%arg);
    }
## TODO how to choose a database?
    my $hashdsn = (keys %{ $client->{databases} })[0];
    my $driver = $client->driver_for($hashdsn);
    $driver->insert($job);

    return TheSchwartz::JobHandle->new({
            dsn_hashed => $hashdsn,
            client     => $client,
            jobid      => $job->jobid
        });
}

sub handle_from_string {
    my $client = shift;
    my $handle = TheSchwartz::JobHandle->new_from_string(@_);
    $handle->client($client);
    return $handle;
}

1;
