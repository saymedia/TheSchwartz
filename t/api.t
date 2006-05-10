# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 13;

my $client = test_client(dbs => ['ts1']);

my $handle;

$handle = $client->insert("feedmajor", { scoops => 2, with => ['cheese','love'] });
isa_ok $handle, 'TheSchwartz::JobHandle', "inserted job";
is($handle->is_pending, 1, "job is still pending");
is($handle->exit_status, undef, "job hasn't exitted yet");

# to give to javascript, perl, etc...
my $hstr = $handle->as_string;    # <digestofdsn>-<jobid>
ok($hstr, "handle stringifies");

my $job = $handle->job;
isa_ok $job, 'TheSchwartz::Job';
is $job->funcname, 'feedmajor', 'handle->job gives us the right job';

# getting a handle object back
my $hand2 = $client->handle_from_string($hstr);
ok($hand2, "handle recreated from stringified version");
is($handle->is_pending, 1, "job is still pending");
is($handle->exit_status, undef, "job hasn't exitted yet");

$job = $handle->job;
isa_ok $job, 'TheSchwartz::Job';
is $job->funcname, 'feedmajor', 'recreated handle gives us the right job';

$job = TheSchwartz::Job->new(
                               funcname => 'feedmajor',
                               run_after=> time() + 60,
                               priority => 7,
                               arg      => { scoops => 2, with => ['cheese','love'] },
                               coalesce => 'major',
                               jobid    => int rand(5000),
                               );
   ok($job);

$handle = $client->insert($job);
isa_ok $handle, 'TheSchwartz::JobHandle';

teardown_dbs('ts1');
