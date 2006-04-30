# $Id$

use strict;
use warnings;
use Test::More tests => 12;

use TheSchwartz;
use Storable;

# With this test, all data structures are in memory so far.  Nothing's
# been inserted into the database because we have no client object
# yet with which to insert.

my $args  = { scoops => 2, with => ['cheese','love'] };
my $fargs = Storable::nfreeze($args);

my $job1 = TheSchwartz::Job->new_from_array("feedmajor", $fargs);
isa_ok($job1, 'TheSchwartz::Job');
my $job2 = TheSchwartz::Job->new_from_array("feedmajor", \$fargs);
isa_ok($job2, 'TheSchwartz::Job');
my $job3 = TheSchwartz::Job->new(funcname => 'feedmajor', arg => $args);
isa_ok($job3, 'TheSchwartz::Job');
my $job4 = TheSchwartz::Job->new(funcname => 'feedmajor', arg => $fargs);
isa_ok($job4, 'TheSchwartz::Job');
my $job5 = TheSchwartz::Job->new(funcname => 'feedmajor', arg => \$fargs);
isa_ok($job5, 'TheSchwartz::Job');

is_deeply($job1->column_values, $job2->column_values, "job2 is equivalent");
is_deeply($job1->column_values, $job3->column_values, "job3 is equivalent");
is_deeply($job1->column_values, $job4->column_values, "job4 is equivalent");
is_deeply($job1->column_values, $job5->column_values, "job5 is equivalent");

my $job6 = TheSchwartz::Job->new(
                                 funcname => 'feeddog',
                                 run_after   => time() + 60,
                                 priority => 7,
                                 arg      => { scoops => 2, with => ['cheese','love'] },
                                 coalesce => 'major',
                                 jobid    => int(rand()*5000),
                                 );
isa_ok $job6, 'TheSchwartz::Job';

my $jobbad = eval { TheSchwartz::Job->new(
                                          funcname => 'feeddog',
                                          run_atter   => time() + 60,  # [sic] typo
                                          ) };
ok(!$jobbad, "no bad job");
ok($@,       "error creating job with bad argument");