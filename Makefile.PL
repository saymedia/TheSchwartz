# $Id$

use 5.008;
use inc::Module::Install;

name('TheSchwartz');
version_from('lib/TheSchwartz.pm');
author('Six Apart <cpan@sixapart.com>');
license('perl');

requires('Data::ObjectDriver' => '0.04');
requires('Digest::MD5');
requires('Storable');

tests( -e "inc/.author" ? "t/*.t xt/*.t" : "t/*.t" );

install_script('bin/schwartzmon');

auto_install();
WriteAll();
