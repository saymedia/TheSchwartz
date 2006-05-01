
use 5.008;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
              'NAME'=> 'TheSchwartz',
              'VERSION' => '0.01',
              'PREREQ_PM'=> {
                Data::ObjectDriver => '0.02',
                            }, # e.g., Module::Name => 1.1
               AUTHOR     => 'Brad Fitzpatrick <brad@danga.com>, Ben Trorr <btrott@sixapart.com>',
             );
