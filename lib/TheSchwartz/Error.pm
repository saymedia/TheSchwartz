# $Id$

package TheSchwartz::Error;
use strict;
use base qw( Data::ObjectDriver::BaseObject );

__PACKAGE__->install_properties({
               columns     => [ qw( jobid message ) ],
               datasource  => 'error',
           });

1;
