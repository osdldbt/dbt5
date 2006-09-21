#!/usr/bin/perl -w

#
# This file is released under the terms of the Artistic License.  Please see
# the file LICENSE, included in this package, for details.
#
# Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
#

use strict;
use Getopt::Long;

my $iostat_data;
my $outdir;

GetOptions(
    "if=s" => \$iostat_data,
    "o=s" => \$outdir
);

unless ( $iostat_data && $outdir ) {
    print "usage: transform_iostat.pl --if <iostat.out> --o <ouput dir>\n";
    exit 1;
}

unless ( -f $iostat_data ) {
    print "$iostat_data doesn't exist\n";
    exit 1;
}

system "mkdir -p $outdir";

my $iostatcsv = $iostat_data . ".csv";
`cat $iostat_data | grep -v '^Device' | grep -v '^\$' | awk '{ print \$1\",\"\$2",\"\$3\",\"\$4\",\"\$5\",\"\$6 }' > $iostatcsv`;

open( FH, "< $iostatcsv" )
    or die "Couldn't open $iostatcsv for reading: $!\n";

my $line;
my %device;

while ( defined( $line = <FH> ) ) {
    chomp $line;
    my @raw_data = split /,/, $line;

    push @{ $device{ $raw_data[ 0 ] }{ tps } }, $raw_data[ 1 ];
    push @{ $device{ $raw_data[ 0 ] }{ blkreadps } }, $raw_data[ 2 ];
    push @{ $device{ $raw_data[ 0 ] }{ blkwrtnps } }, $raw_data[ 3 ];
    push @{ $device{ $raw_data[ 0 ] }{ blkread } }, $raw_data[ 4 ];
    push @{ $device{ $raw_data[ 0 ] }{ blkwrtn } }, $raw_data[ 5 ];
}

my @word = split /\//, $iostat_data;
my $in_filename = $word[ $#word ];

for my $devname ( keys %device ) {
    # Some iostat converts '/' to '!', like in sysfs, some don't.
    # Always do it here.
    my $dev = $devname;
    $dev=~ s/\//!/;
    my $out_filename = "$outdir/$in_filename" . "." . $dev. ".data";
    my $count = scalar( @{$device{ $devname }{ tps } } );

    # Start at 1 because 0 appears to have bogus data.
    for ( my $i = 1; $i < $count; $i++ ) {
        system "echo \"$i ${$device{ $devname }{ tps } }[ $i ] "
            . "${$device{ $devname }{ blkreadps } }[ $i ] "
            . "${$device{ $devname }{ blkwrtnps } }[ $i ] "
            . "${$device{ $devname }{ blkread } }[ $i ] "
            . "${$device{ $devname }{ blkwrtn } }[ $i ]\" >> $out_filename";
    }
}
