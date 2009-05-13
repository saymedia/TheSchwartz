# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use Test::More tests => 30;

use TheSchwartz;
use File::Spec qw();
use File::Temp qw(tempdir);
# create a tmp directory with a unique name.  This stops
# us conflicting with any other runs of this process and means
# we tidy up after ourselves
my $tempdir = tempdir( CLEANUP => 1 );

run_tests(10, sub {
    my $pfx = '';
    my $dbs = ['ts1'];

    setup_dbs({prefix => $pfx}, $dbs);

    my $client = TheSchwartz->new(scoreboard => $tempdir,
                                  databases => [
                                          map { {
                                              dsn  => dsn_for($_),
                                              user => $ENV{TS_DB_USER},
                                              pass => $ENV{TS_DB_PASS},
                                              prefix => $pfx,
                                          } } @$dbs
                                          ]);

    my $sb_file = $client->scoreboard;
    {
        (undef, my ($sb_dir, $sb_name))  = File::Spec->splitpath($sb_file);
        ok(-e $sb_dir, "Looking for dir $sb_dir");
    }

    {
        my $handle = $client->insert("Worker::Addition",
                                     {numbers => [1, 2]});
        my $job    = Worker::Addition->grab_job($client);

        my $rv = eval { Worker::Addition->work_safely($job); };
        ok(length($@) == 0, 'Finished job with out error')
            or diag($@);

        unless (ok(-e $sb_file, "Scoreboard file exists")) {
            return;
        }

        open(FH, $sb_file) or die "Can't open '$sb_file': $!\n";

        my %info = map { chomp; /^([^=]+)=(.*)$/ } <FH>;
        close(FH);

        ok($info{pid} == $$, 'Has our PID');
        ok($info{funcname} eq 'Worker::Addition', 'Has our funcname');
        ok($info{started} =~ /\d+/, 'Started time is a number');
        ok($info{started} <= time, 'Started time is in the past');
        ok($info{arg} =~ /^numbers=ARRAY/, 'Has right args');
        ok($info{done} =~ /\d+/, 'Job has done time');
    }

    {
        $client->DESTROY;
        ok(! -e $sb_file, 'Scoreboard file goes away when worker finishes');
    }

    teardown_dbs('ts1');
});

############################################################################
package Worker::Addition;
use base 'TheSchwartz::Worker';

sub work {
    my ($class, $job) = @_;

    # ....
}

1;
