# -*-perl-*-

use strict;
use warnings;
use TheSchwartz;
use Test::More 'no_plan';

my $client = TheSchwartz->new(databases => [
                                            {
                                                dsn  => "dbi:SQLite:dbname=ts1.sqlite",
                                                user => "",
                                                pass => "",
                                            },
                                            ]);
my $handle;

$handle = $client->insert("feedmajor", scoops => 2, with => ['cheese','love']);
ok($handle, "got a job handle");
is($handle->is_pending, "pending", "job is still pending");
is($handle->exit_status, undef, "job hasn't exitted yet");

# to give to javascript, perl, etc...
my $hstr = $handle->as_string;    # <digestofdsn>-<jobid>
ok($hstr, "handle stringifies");

# getting a handle object ack
my $hand2 = $client->handle_from_string($hstr);
ok($hand2, "handle recreated from stringified version");
is($handle->is_pending, "pending", "job is still pending");
is($handle->exit_status, undef, "job hasn't exitted yet");


my $job = TheSchartz::Job->new(
                               funcname => 'feedmajor',
                               run_at   => time() + 60,
                               priority => 7,
                               arg      => { scoops => 2, with => ['cheese','love'] },
                               coalesce => 'major',
                               jobid    => rand(),
                               );
   ok($job);

$handle = $client->insert($job);
   ok($handle);



