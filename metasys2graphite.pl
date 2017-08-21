#!/usr/bin/perl
#
#

################################################################################
# metasys2graphite.pl
#   a utility to convert metasys .DBF files to graphite data
################################################################################
# T. Czerwonka <tczerwonka@gmail.com>
################################################################################
# $Id: metasys2graphite.pl,v 1.12 2017/07/11 02:27:22 timc Exp $
################################################################################

use strict;
use File::Find;
use Data::Dumper;
#http://search.cpan.org/~janpaz/DBD-XBase-1.05/lib/XBase.pm
use XBase;
use DateTime;
use IO::Socket::INET;

my $year;
my $month; 
my $day;
my $hour;
my $minute;
#if you define this here you get carried-over numbers
#my $sensorval;
my $debug = 0;

my $carbon_port = 2003;
#my $carbon_host = '127.0.0.1';
my $carbon_host = '208.66.130.113';
my $carbon_proto = 'tcp';
my $logfile = '/var/log/xbee-s6b.log';


#dataroot is root of the hierarchy where the DBF files are stored
#my $dataroot = '/home/timc/metasys2/Project';
my $dataroot = '/local.1/metasys';
my %graphitedata = ();

# The socket on which the Carbon (graphite) server is listening.
#my $socket = new IO::Socket::INET (
#  PeerHost => $carbon_host,
#  PeerPort => $carbon_port,
#  Proto => $carbon_proto,
#) or die "Error in Socket Creation : $!\n";

# Open the log file.
#open my $file, ">>$logfile" or die $!;

#create a hash of all DBF files under $dataroot -- maybe there are multiple

#read in the directories
my @directories = ($dataroot);
my @foundfiles;
find(
	sub { 
	push @foundfiles, $File::Find::name if /\d+\.DBF$/ 
	}, @directories );


#make a hash -- key for the hash is the path, transformed to strip the $dataroot off
#and the /nnn.DBF at the end
foreach my $tag (@foundfiles) {
	my $fspath = $tag;	
	#lop off the /125.DBF
	#$tag =~ s/\/\d+\.DBF//g;
	#actually everything -- there is a TOTAL.DBF
	#$tag =~ s/\/\w+\.DBF//g;
	#$tag =~ s/$\.DBF//g;

        #some dirs contain multiple DBF files -- add the DBF name to the previous dir
        #CAUTION -- you don't want to change structure of existing data...
        if ($tag =~ /ALL-PTS/) {
                $tag =~ s/.DBF//g;
        }
        if ($tag =~ /\/\w+\.DBF/) {
                $tag =~ s/\/\w+\.DBF//g;
        }
        if ($tag =~ /$\.DBF/) {
                $tag =~ s/$\.DBF//g;
        }
     

	#lop off the front
	$tag =~ s/$dataroot\///g;
	#take off the 'Mesls'
	$tag =~ s/Mesls\///g;
	#convert slashes to dots
	$tag =~ s/\//./g;
	#now make a hash of this
	$graphitedata{$tag}{filename} = $fspath;
} #foreach

#print Dumper(%graphitedata);



#iterate through the hash, load all of this shit up
my %options;
my $epochtime;
#VALREAL,DATE_NDX,TIME_NDX
$options{fields} = 'DATE_NDX,TIME_NDX,VALREAL';
foreach my $sensor (reverse sort keys %graphitedata) {
	#open/close socket for each sensor
	# The socket on which the Carbon (graphite) server is listening.
	my $socket = new IO::Socket::INET (
	  PeerHost => $carbon_host,
	  PeerPort => $carbon_port,
	  Proto => $carbon_proto,
	) or die "Error in Socket Creation : $!\n";
	print "opened socket for $sensor\n";
	$debug && print $graphitedata{$sensor}{filename};
	$debug && print "\n";
	#open file
	my $file = $graphitedata{$sensor}{filename};

	my $table = new XBase "$file" or die XBase->errstr;
	my $cursor = $table->prepare_select("DATE_Y", "DATE_M", "DATE_D", "TIME_H", "TIME_M", "VALFLOAT", "FPVALUE", "VALREAL");

    	while (my @data = $cursor->fetch) {
		my $sensorval = 0;
        	### do something here, like print "@data\n";
		($year, $month, $day, $hour, $minute, my $valfloat, my $fpvalue, my $valreal) = @data;
		#y2k problem!
		$year += 1900;
		$valfloat && ($sensorval = $valfloat);
		$fpvalue && ($sensorval = $fpvalue);
		$valreal && ($sensorval = $valreal);

		if ($year > 1900) {
			my $dt = DateTime->new(
    				year       => $year,
    				month      => $month,
    				day        => $day,
    				hour       => $hour,
    				minute     => $minute,
				);	
				$epochtime = $dt->epoch;
			}
		#ratchet the epochtime forward by 9 hours:
		#actual reading taken at 6:45 a.m.
		#metasys claims reading done at 2:45 am
		#unixdate epoch indicates 9:45 p.m. yesterday when jammed in graphite
		#ergo -- add 9 hours worth of seconds
	 	#$epochtime += 32400;	
		#data appears to be ahead by 4 hours -- so instead of 9 hours worth of
		#seconds, go ahead 5 hours worth of seconds
	 	$epochtime += 18000;	
		#only do something if a sensor value exists
		if($sensorval) {
			#print "$sensor $sensorval $epochtime\n";
  			insert($socket, $sensor, $sensorval, $epochtime);
		}
        } #while
	$table->close;
	$socket->close();
	
}








# Send the sensor value, sensor name and timestamp to the socket and log file.
sub insert {
  my ($socket, $sensor_id, $sensor_val, $epochtime) = @_;
  my $data = "$sensor_id $sensor_val $epochtime\n";
  print $socket $data;
  #print $file $data;
  $debug && print $data;
}

#close $file;


