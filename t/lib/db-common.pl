# $Id$

use strict;
use File::Spec;

sub db_filename {
    my($dbname) = @_;
    $dbname . '.db';
}

sub dsn_for {
    'dbi:SQLite:dbname=' . db_filename(@_);
}

sub setup_dbs {
    my($schema, $dbs) = @_;
    teardown_dbs(@$dbs);
    for my $dbname (@$dbs) {
        my $dbh = DBI->connect(dsn_for($dbname),
            '', '', { RaiseError => 1, PrintError => 0 });
        my @sql = load_sql($schema);
        for my $sql (@sql) {
            $dbh->do($sql);
        }
        $dbh->disconnect;
    }
}

sub teardown_dbs {
    my(@dbs) = @_;
    for my $db (@dbs) {
        my $file = db_filename($db);
        next unless -e $file;
        unlink $file or die "Can't teardown $db: $!";
    }
}

sub load_sql {
    my($file) = @_;
    open my $fh, $file or die "Can't open $file: $!";
    my $sql = do { local $/; <$fh> };
    close $fh;
    split /;\s*/, $sql;
}

1;
