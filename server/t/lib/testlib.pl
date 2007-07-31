# $Id: db-common.pl 91 2006-08-17 00:39:55Z bradfitz $

use strict;
use File::Spec;
use Carp qw(croak);
use DBI;
use FindBin;
use JSON::Any;

use lib "$ENV{HOME}/hack/Data-ObjectDriver/lib";
use lib "$ENV{HOME}/hack/TheSchwartz/lib";
use lib "$ENV{HOME}/hack/gearman/api/perl/Gearman/lib";
use lib "$ENV{HOME}/cvs/Data-ObjectDriver/lib";
use lib "$ENV{HOME}/cvs/TheSchwartz/lib";
use lib "$ENV{HOME}/cvs/gearman/api/perl/Gearman/lib";

sub json {
    return JSON::Any->objToJson(shift);
}

sub unjson {
    return JSON::Any->json_to_obj(shift);
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

package TestDB;
use strict;
sub new {
    my $class = shift;
    my $name = shift || "unnamed";
    my $db = TestDB::MySQL->new($name) || TestDB::SQLite->new($name);
    if ($db) {
	my $dbh = $db->dbh;
	my $schema = $db->schema_file;
        my @sql = _load_sql($schema);
        for my $sql (@sql) {
	    $db->alter_create(\$sql);
            $dbh->do($sql);
        }
        $dbh->disconnect;
	return $db;
    }

    eval {
	Test::More::plan(skip_all => "MySQL or SQLite not available for testing");
    };
    if ($@) {
	return undef;
    }
    exit(0);
}

sub dbh {
    my ($self) = @_;
    return DBI->connect($self->dsn, "root", "", { RaiseError => 1 });
}

sub alter_create {
    my $sqlref = shift;
    # subclasses can override
}

sub _load_sql {
    my($file) = @_;
    open my $fh, $file or die "Can't open $file: $!";
    my $sql = do { local $/; <$fh> };
    close $fh;
    split /;\s*/, $sql;
}

package TestDB::MySQL;
use strict;
use base 'TestDB';

sub new {
    my ($class, $name) = @_;

    my $dbh  = eval { _mysql_dbh() } or return undef;
    my $self = bless {
	basename  => $name,
	dbname    => "t_sch_$name",
	root_dbh  => $dbh,
    }, $class;

    $dbh->do("DROP DATABASE IF EXISTS $self->{dbname}");
    $dbh->do("CREATE DATABASE $self->{dbname}");
    return $self;
}

sub dsn {
    my ($self) = @_;
    return "DBI:mysql:" . $self->{dbname};
}

sub _mysql_dbh {
    return DBI->connect("DBI:mysql:mysql", "root", "", { RaiseError => 1 })
        or die "Couldn't connect to database";
}

sub alter_create {
    my ($self, $sqlref) = @_;
    $$sqlref .= " ENGINE=INNODB\n";
}

sub schema_file {
    return "../doc/schema.sql";
}

package TestDB::SQLite;
use strict;
use base 'TestDB';

sub new {
    return undef;
}

package TestServer;
use strict;

sub new {
    my ($class, $db) = @_;
    $db ||= TestDB->new || return undef;
    my $pid = fork;
    die "out of memory" unless defined $pid;
    if ($pid) {
	return bless {
	    pid => $pid,
	}, $class;
    }

    my $bin = "$FindBin::Bin/../bin/schwartzd";
    die "Not exist: $bin" unless -e $bin;
    die "Not executable: $bin" unless -x $bin;
    exec $bin;
    die "Failed to exec test schwartzd!";
}

sub gearman_client {
    my $self = shift;
    my $cl = Gearman::Client->new;
    $cl->job_servers('127.0.0.1:7003');
    return $cl;
}

sub DESTROY {
    my $self = shift;
    if ($self->{pid}) {
	kill 9, $self->{pid};
    }
}

1;
