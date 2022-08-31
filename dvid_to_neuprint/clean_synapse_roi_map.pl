#!/usr/bin/perl

use strict;

my $file = $ARGV[0];
my $synCount = 0;
print "synId,x,y,z,type,confidence,toplevel_roi,sub1_roi,sub2_roi,sub3_roi\n";
open(IN,"$file");
while (my $line = <IN>) {
    if ($line =~ /^\d+/) {
	chomp($line);
	$line =~ s/\<unspecified\>//g;
	my @data = split(/\,/,$line);
    
	$synCount++;
	my $synId = 99000000000 + $synCount;
	print "$synId,$data[0],$data[1],$data[2],$data[3],$data[4],$data[7],,,\n";
    }
}
close(IN);
