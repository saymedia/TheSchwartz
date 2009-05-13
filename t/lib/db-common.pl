# $Id$

use strict;
use File::Spec;
use Carp qw(croak);

sub run_tests {
    my ($n, $code) = @_;

    run_tests_mysql($n, $code);
    run_tests_pgsql($n, $code);
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
      local $ENV{TS_DB_USER} ||= 'root';
      my $dbh = eval { mysql_dbh() };
      skip "MySQL not accessible as root on localhost", $n if $@;
      skip "InnoDB not available on localhost's MySQL", $n if $innodb && ! has_innodb($dbh);
      $code->();
  }
}

sub run_tests_pgsql {
    my ($n, $code) = @_;
  SKIP: {
      local $ENV{USE_PGSQL} = 1;
      local $ENV{TS_DB_USER} ||= 'postgres';
      my $dbh = eval { pgsql_dbh() };
      skip "PgSQL not accessible as root on localhost", $n if $@;
      $code->();
  }
}

sub run_tests_sqlite {
    my ($n, $code) = @_;

    # SQLite
  SKIP: {
      my $rv = eval "use DBD::SQLite; 1";
      $rv = 0 if $ENV{SKIP_SQLITE};
      skip "SQLite not installed", $n if !$rv;
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

    if ($ENV{USE_DBH_FOR_TEST}) {
        my @tmp;
        for (@$dbs) { eval {
            my $dsn    = dsn_for($_);  
            my $dbh    = DBI->connect( $dsn, "root", "", {
                RaiseError => 1,
                PrintError => 0,
                AutoCommit => 1,
            } ) or die $DBI::errstr;
            my $driver =  Data::ObjectDriver::Driver::DBI->new( dbh => $dbh); 
            push @tmp, { driver => $driver, prefix => $pfx };
        } }
        return TheSchwartz->new(databases => [@tmp]); 
    } else {
        return TheSchwartz->new(databases => [
                                          map { {
                                              dsn  => dsn_for($_),
                                              user => $ENV{TS_DB_USER},
                                              pass => $ENV{TS_DB_PASS},
                                              prefix => $pfx,
                                          } } @$dbs
                                          ]);
    }
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
    return "doc/schema-postgres.sql" if $ENV{USE_PGSQL};
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
    }
    elsif ($ENV{USE_PGSQL}) {
        return 'dbi:Pg:dbname=' . mysql_dbname($dbname);
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
        elsif ($ENV{USE_PGSQL}) {
            create_pgsql_db(mysql_dbname($dbname));
        }
        my $dbh = DBI->connect(dsn_for($dbname),
            $ENV{TS_DB_USER}, $ENV{TS_DB_PASS}, { RaiseError => 1, PrintError => 0 })
            or die "Couldn't connect: $!\n";
        my @sql = load_sql($schema);
        for my $sql (@sql) {
            $sql =~ s!^\s*create\s+table\s+(\w+)!CREATE TABLE ${pfx}$1!mi;
            $sql =~ s!^\s*(create.*?index)\s+(\w+)\s+on\s+(\w+)!$1 $2 ON ${pfx}$3!i;
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

my $pg_dbh;

sub pgsql_dbh {
    return $pg_dbh if $pg_dbh;
    $pg_dbh ||=
        DBI->connect("DBI:Pg:dbname=postgres", "postgres", "", { RaiseError => 1 })
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

sub create_pgsql_db {
    my $dbname = shift;
    pgsql_dbh()->do("CREATE DATABASE $dbname");
}

sub drop_pgsql_db {
    my $dbname = shift;
    undef $pg_dbh;
    eval { pgsql_dbh()->do("DROP DATABASE IF EXISTS $dbname") };
}

sub teardown_dbs {
    my(@dbs) = @_;
    for my $db (@dbs) {
        if ($ENV{USE_MYSQL}) {
            drop_mysql_db(mysql_dbname($db));
        } elsif ($ENV{USE_PGSQL}) {
            drop_pgsql_db(mysql_dbname($db));
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
