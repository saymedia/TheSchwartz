# -*-perl-*-

use strict;
use warnings;
use Test::More;
require 't/lib/testlib.pl';

my $db = TestDB->new;
plan tests => 1;

ok($db, "got a test database");

my $srv = TestServer->new($db);
ok($srv, "got a test server");


