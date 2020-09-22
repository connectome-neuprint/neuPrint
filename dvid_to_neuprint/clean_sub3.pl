#!/usr/bin/perl

use strict;

my $file = $ARGV[0];

print "synId,x,y,z,type,confidence,toplevel_roi,sub1_roi,sub2_roi,sub3_roi\n";
open(IN,"$file");
while (my $line = <IN>) {
    if ($line =~ /^\d+/) {
	chomp($line);
	$line =~ s/\<unspecified\>//g;
	my @data = split(/\,/,$line);
    
	#if ($data[6] eq "<unspecified>") {
	#$data[6] = ""
	#}

	print "$data[0],$data[1],$data[2],$data[3],$data[4],$data[5],$data[6],$data[7],$data[8],$data[10]\n";
    }
}
close(IN);
