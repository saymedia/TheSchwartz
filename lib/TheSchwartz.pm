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

    $client;
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
    Data::ObjectDriver::Driver::DBI->new(
            dsn      => $db->{dsn},
            username => $db->{user},
            password => $db->{pass},
        );
}

sub lookup_job {
    my $client = shift;
    my $handle = $client->handle_from_string(@_);
    my $driver = $client->driver_for($handle->dsn_hashed);
    $driver->lookup('TheSchwartz::Job' => $handle->jobid);
}

sub insert {
    my $client = shift;
    my($job) = @_;
    unless (ref($job) eq 'TheSchwartz::Job') {
        $job = TheSchwartz::Job->new_from_array($job);
    }
## TODO how to choose a database?
    my $hashdsn = (keys %{ $client->{databases} })[0];
    my $driver = $client->driver_for($hashdsn);
    $driver->insert($job);

    TheSchwartz::JobHandle->new({
            dsn_hashed => $hashdsn,
            client     => $client,
            jobid      => $job->jobid
        });
}

sub handle_from_string {
    my $client = shift;
    my $handle = TheSchwartz::JobHandle->new_from_string(@_);
    $handle->client($client);
    $handle;
}

1;
