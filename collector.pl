#!/usr/bin/perl

use strict;
use warnings;

sub process_dat {
	my $blob = shift;
	my ($mac, $ip, $date, $name, $iface) = split(/\s+/, $blob);
}
