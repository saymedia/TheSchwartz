Name: perl-TheSchwartz
Version: 0.01
Release: 1
Summary: Reliable distributed job system
License: perl
Group: Applications/Internet
BuildRoot: %{_tmppath}/%name-%version-root
AutoReqProv: no
Packager: <cpan@sixapart.com>

%description

%prep

%build
rm -rf trunk
svn export http://code.sixapart.com/svn/TheSchwartz/trunk
cd trunk
%{__perl} Makefile.PL PREFIX=%{buildroot}%{_prefix}
make

%install
rm -rf %{buildroot}
cd trunk
make install
rm -rf %{buildroot}/%{_prefix}/lib64

%clean
rm -rf %{buildroot}

%files
%{_bindir}/*
%{_prefix}/lib/*
#%{_mandir}/*
