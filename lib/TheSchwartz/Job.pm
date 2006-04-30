# $Id$

package TheSchwartz::Job;
use strict;
use base qw( Data::ObjectDriver::BaseObject );

use Storable ();
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

sub new_from_array {
    my $class = shift;
    my(@arg) = @_;
    $class->new(
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
    $job;
}

1;
