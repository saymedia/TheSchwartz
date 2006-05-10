# $Id$

use strict;
use File::Spec;
use Carp qw(croak);

sub test_client {
    my %opts = @_;
    my $dbs     = delete $opts{dbs};
    my $init    = delete $opts{init};
    croak "'dbs' not an ARRAY" unless ref $dbs eq "ARRAY";
    croak "unknown opts" if %opts;
    $init = 1 unless defined $init;

    if ($init) {
        setup_dbs(schema_file() => $dbs);
    }

    return TheSchwartz->new(databases => [
                                          map { {
                                              dsn  => dsn_for($_),
                                              user => "root",
                                              pass => "",
                                          } } @$dbs
                                          ]);
}

sub schema_file {
    return "doc/schema.sql" if $ENV{USE_MYSQL};
    return "t/schema-sqlite.sql";
}

sub db_filename {
    my($dbname) = @_;
    return $dbname . '.db';
}

sub mysql_dbname {
    my($dbname) = @_;
    return 't_sch_' . $dbname;
}

sub dsn_for {
    my $dbname = shift;
    if ($ENV{USE_MYSQL}) {
        return 'dbi:mysql:' . mysql_dbname($dbname);
    } else {
        return 'dbi:SQLite:dbname=' . db_filename($dbname);
    }
}

sub setup_dbs {
    my($schema, $dbs) = @_;
    $schema = "doc/schema.sql" if $ENV{USE_MYSQL};
    teardown_dbs(@$dbs);
    for my $dbname (@$dbs) {
        if ($ENV{USE_MYSQL}) {
            create_mysql_db(mysql_dbname($dbname));
        }
        my $dbh = DBI->connect(dsn_for($dbname),
            'root', '', { RaiseError => 1, PrintError => 0 })
            or die "Couldn't connect: $!\n";
        my @sql = load_sql($schema);
        for my $sql (@sql) {
            $dbh->do($sql);
        }
        $dbh->disconnect;
    }
}

sub mysql_dbh {
    return DBI->connect("DBI:mysql:mysql", "root", "", { RaiseError => 1 })
        or die "Couldn't connect to database";
}

sub create_mysql_db {
    my $dbname = shift;
    mysql_dbh()->do("CREATE DATABASE $dbname");
}

sub drop_mysql_db {
    my $dbname = shift;
    mysql_dbh()->do("DROP DATABASE IF EXISTS $dbname");
}

sub teardown_dbs {
    my(@dbs) = @_;
    for my $db (@dbs) {
        if ($ENV{USE_MYSQL}) {
            drop_mysql_db(mysql_dbname($db));
        } else {
            my $file = db_filename($db);
            next unless -e $file;
            unlink $file or die "Can't teardown $db: $!";
        }
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
