package TheSchwartz::Job;
use Data::ObjectDriver::Driver::Partition;

use base qw( Data::ObjectDriver::BaseObject );
__PACKAGE__->install_properties({
               columns     => [qw(jobid funcname args uniqkey insert_time
                                  run_after grabbed_util priority setname)],
               datasource  => 'job',
               primary_key => 'jobid',
               driver      => Data::ObjectDriver::Driver::Partition->new(get_driver => \&get_driver),
           });

sub driver {
    my $self = shift;
    my $driver =$self->client->driver_for($self->{uniqkey});
    $self->{_lastdriver} = $driver;
    return $driver;
}

sub lookup {
    my ($class, $handle) = @_;
    my ($hashdsn, $jobid) = split(/\-/, $handle);
    my @clients = TheSchwartz->clients;
    foreach my $c (@clients) {
        my @dsn = $c->dsns;

    }
}

sub get_driver {

}

                                   get_driver => \&get_driver,
                               ),




sub new
{ }

package TheSchwartz;
sub lookup_job {
    my $client = shift;
    my($handle) = @_;
    my($hashdsn, $jobid) = split /\-/, $handle;
    my $driver = $client->driver_for($hashdsn);
    my $job = $driver->lookup('TheSchwartz::Job' => $jobid);
    return $job;
}

sub driver_for {
    my $client = shift;

}
