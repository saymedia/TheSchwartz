# -*-perl-*-

use strict;
use warnings;
use TheSchwartz;


my $client = TheSchwartz->new(dsns => []);



$client->insert("feedmajor", scoops => 2, with => ['cheese','love']);
$client->insert(
                TheSchartz::Job->new(
                                     funcname => 'feedmajor',
                                     run_at   => time() + 60,
                                     priority => 7,
                                     arg      => { scoops => 2, with => ['cheese','love'] },
                                     coalesce => 'major',
                                     jobid    => rand(),
                                     )
                );



