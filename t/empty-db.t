# $Id$
# -*-perl-*-

use strict;
use warnings;

require 't/lib/db-common.pl';

use TheSchwartz;
use Test::More tests => 9;

run_tests(3, sub {
    teardown_dbs("tempty1");

    my $client = TheSchwartz->new(databases => [
                                                {
                                                    dsn  => dsn_for('tempty1'),
                                                    user => $ENV{TS_DB_USER},
                                                    pass =>  $ENV{TS_DB_PASS},
                                                },
                                                ]);

    # insert a job
    {
        my $handle;
        $handle = $client->insert("Worker::Addition", { numbers => [1, 2] });
        ok(!$handle, "can't insert into empty database");
        $handle = $client->insert("Worker::Addition", { numbers => [1, 2] });
        ok(!$handle, "still can't insert into empty database");
    }

    ok(1, "test finishes");
    teardown_dbs("tempty1");
});

