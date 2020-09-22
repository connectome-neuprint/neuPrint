#!/usr/bin/perl

use strict;

my $file = $ARGV[0];

print "synId,x,y,z,type,confidence,toplevel_roi\n";
my $synCount = 0;
open(IN,"$file");
while (my $line = <IN>) {
    if ($line =~ /^\d+/) {
	chomp($line);
	
	my @data = split(/\,/,$line);
    
	#if ($data[6] eq "<unspecified>") {
	#$data[6] = ""
	#}
	$synCount++;
	my $synId = 99000000000 + $synCount;
	print "$synId,$data[0],$data[1],$data[2],$data[3],$data[4],$data[7]\n";
    }
}
close(IN);
