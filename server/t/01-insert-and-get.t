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

print Dumper($cl->do_task("insert_job", json({
    func => "foo",
    arg => "fooarg",
})));
