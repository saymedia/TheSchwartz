# $Id$

use strict;
use File::Spec;
use Carp qw(croak);

sub run_tests {
    my ($n, $code) = @_;

    run_tests_mysql($n, $code);
    run_tests_sqlite($n, $code);
}

sub run_tests_innodb {
    my ($n, $code) = @_;
    run_tests_mysql($n, $code, 1);
}

sub run_tests_mysql {
    my ($n, $code, $innodb) = @_;
  SKIP: {
      local $ENV{USE_MYSQL} = 1;
      my $dbh = eval { mysql_dbh() };
      skip "MySQL not accessible as root on localhost", $n if $@;
      skip "InnoDB not available on localhost's MySQL", $n if $innodb && ! has_innodb($dbh);
      $code->();
  }
}

sub run_tests_sqlite {
    my ($n, $code) = @_;

    # SQLite
  SKIP: {
      my $rv = eval "use DBD::SQLite; 1";
      skip "SQLite not installed", $n if $@;
      $code->();
  }
}

sub test_client {
    my %opts = @_;
    my $dbs     = delete $opts{dbs};
    my $init    = delete $opts{init};
    my $pfx     = delete $opts{dbprefix};
    croak "'dbs' not an ARRAY" unless ref $dbs eq "ARRAY";
    croak "unknown opts" if %opts;
    $init = 1 unless defined $init;

    if ($init) {
        setup_dbs({ prefix => $pfx }, $dbs);
    }

    return TheSchwartz->new(databases => [
                                          map { {
                                              dsn  => dsn_for($_),
                                              user => "root",
                                              pass => "",
                                              prefix => $pfx,
                                          } } @$dbs
                                          ]);
}

sub has_innodb {
    my $dbh = shift;
    my $tmpname = "test_to_see_if_innoavail";
    $dbh->do("CREATE TABLE IF NOT EXISTS $tmpname (i int) ENGINE=INNODB")
        or return 0;
    my @row = $dbh->selectrow_array("SHOW CREATE TABLE $tmpname");
    my $row = join(' ', @row);
    my $has_it = ($row =~ /=InnoDB/i);
    $dbh->do("DROP TABLE $tmpname");
    return $has_it;
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
    shift if $_[0] =~ /\.sql$/;  # skip filenames (old)

    my $opts = ref $_[0] eq "HASH" ? shift : {};
    my $pfx = delete $opts->{prefix} || "";
    die "unknown opts" if %$opts;

    my(@dbs) = @_;
    my $dbs = ref $dbs[0] ? $dbs[0] : \@dbs;  # support array or arrayref (old)

    my $schema = schema_file();
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
            $sql =~ s!^\s*create\s+table\s+(\w+)!CREATE TABLE ${pfx}$1!i;
            $sql .= " ENGINE=INNODB\n" if $ENV{USE_MYSQL};
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
