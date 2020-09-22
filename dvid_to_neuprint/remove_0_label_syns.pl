#!/usr/bin/perl

use strict;

my $file = $ARGV[0];

#print "synId,x,y,z,type,confidence,super_roi,sub1_roi,sub2_roi,sub3_roi\n";
print "synId,x,y,z,type,confidence,toplevel_roi,sub1_roi,sub2_roi,sub3_roi,body\n";
open(IN,"$file");
while (my $line = <IN>) {
    if ($line =~ /^\d+/) {
	chomp($line);
	#$line =~ s/\<unspecified\>//g;
	my @data = split(/\,/,$line);
    
	#if ($data[6] eq "<unspecified>") {
	#$data[6] = ""
	#}
	#print "$data[10]\n";
	if ($data[10] != 0) {
	    print "$line\n";
	}
    }
}
close(IN);
