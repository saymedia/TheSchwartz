# -*-perl-*-

use strict;
use warnings;
use Test::More;
BEGIN {
    require 't/lib/testlib.pl';
}
use Gearman::Client;
use Data::Dumper;

my $db = TestDB->new;
plan tests => 1;

ok($db, "got a test database");

my $srv = TestServer->new($db);
ok($srv, "got a test server");

my $cl = $srv->gearman_client;

my $ret;

# FIXME: test currently requires running gearmand on localhost
{
    use IO::Socket::INET;
    my $sock = IO::Socket::INET->new(PeerAddr => "127.0.0.1:7003");
    ok($sock, "local gearmand is up for testing")
        or die "can't continue";
}

sub do_req {
    my $req = shift;
    my $ret = $cl->do_task("insert_job", json($req));
    return undef unless $ret;
    return $$ret unless $$ret =~ /^\s*[\[\{]/;
    return unjson($$ret);
}

$ret = do_req({
    funcname => "foo",
    arg => "fooarg",
});
like($ret, qr/^\w+-\d+$/, "got a job handle");

