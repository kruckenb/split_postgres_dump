#!/usr/bin/perl

# split-pg-dump.pl
# Separate output from pg_dump into multiple files (dump.####.<name>)
#
# Intended to maximize compression of de-dup'ing backup system
#
# Theory: By separating table data into individual files, (1) de-dup'ing of
# unchanged tables is very high, unaffected by other tables; (2) tables
# with new (appended) records can still be highly compressed because
# older records (positioned earlier in the dump) are de-dup'ed; (3) only
# new tables and tables with changed rows have lower compression with
# less de-dup'ing
#
# Files are numbered to maintain order for reconstruction
# First file (dump.0000.pre-schema) contains first part of schema information
# Next files contain table data, in same order as pg_dump output
# Last file (dump.####.post-schema) contains last part of schema information
#
# Use: pg_dump | split-pg-dump.pl
#
# To reconstruct complete dump (for restoring, etc)
#   cat dump.* > dump.txt

use strict;
use warnings;

my ($count, $pause_for_break_check, $fh, @buffer);
my $out = make_filename('pre-schema');
my $state = 'pre';
while (<>) {
  # Buffer line so output can be controlled
  push @buffer, $_;

  # Watch for 'break' sections that might change output
  if (/^--\s*$/ && $state ne 'post') {
    $pause_for_break_check = 1;
    next;
  }
  if ($pause_for_break_check) {
    if (/^-- Data for Name: (\S+); Type: TABLE DATA;/) {
      # Data for a new table, start a new file
      $out = make_filename($1);
      $state = 'data';
    } elsif (/^-- Name/ && $state ne 'pre') {
      # Post-data schema information
      $out = make_filename('post-schema');
      $state = 'post';
    } else {
      # Don't need to change output
      $pause_for_break_check = 0;
    }
  }

  # Init output file
  if ($out) {
    close $fh if $fh;
    $fh = undef;

    open $fh, "> $out";
    $out = undef;
  }

  # Force close (and buffering) if next section starts
  if ($state eq 'data-end' && /\S/) {
    close $fh;
    $fh = undef;
  }

  # Output any lines
  if (!$pause_for_break_check && $fh) {
    print $fh @buffer;
    @buffer = ();
  }

  # Detect end-of-data
  if ($state eq 'data' && /^\\\.$/) {
    $state = 'data-end';
  }
}

# Clean up
if ($fh) {
  print $fh @buffer;
  close $fh;
}

sub make_filename {
  my $ext = shift;

  return 'dump.' . sprintf('%.4d', $count++) . ".$ext";
}
