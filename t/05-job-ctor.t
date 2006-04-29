# -*-perl-*-

use strict;
use warnings;
use TheSchwartz;
use Test::More 'no_plan';
use Storable;

# With this test, all data structures are in memory so far.  Nothing's
# been inserted into the database because we have no client object
# yet with which to insert.

my $args  = { scoops => 2, with => ['cheese','love'] };
my $fargs = Storable::nfreeze($args);

my $job1 = TheSchwartz::Job->new_from_array("feedmajor", $fargs);
ok($job1, "simple ctor");
is(ref $job1, "TheSchwartz::Job", "is an object");
my $job2 = TheSchwartz::Job->new_from_array("feedmajor", \$fargs);
ok($job2);
my $job3 = TheSchwartz::Job->new(funcname => 'feedmajor', arg => $args);
ok($job3);
my $job4 = TheSchwartz::Job->new(funcname => 'feedmajor', arg => $fargs);
ok($job4);
my $job5 = TheSchwartz::Job->new(funcname => 'feedmajor', arg => \$fargs);
ok($job5);

is_deeply($job1, $job2, "job2 is equivalent");
is_deeply($job1, $job3, "job3 is equivalent");
is_deeply($job1, $job4, "job4 is equivalent");
is_deeply($job1, $job5, "job5 is equivalent");

my $job6 = TheSchwartz::Job->new(
                                 funcname => 'feeddog',
                                 run_after   => time() + 60,
                                 priority => 7,
                                 arg      => { scoops => 2, with => ['cheese','love'] },
                                 coalesce => 'major',
                                 jobid    => int(rand()*5000),
                                 );
ok($job6);

my $jobbad = eval { TheSchwartz::Job->new(
                                          funcname => 'feeddog',
                                          run_atter   => time() + 60,  # [sic] typo
                                          ) };
ok(!$jobbad, "no bad job");
ok($@,       "error creating job with bad argument");








