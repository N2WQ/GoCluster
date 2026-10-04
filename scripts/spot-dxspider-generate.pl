#!/usr/bin/env perl
# Sender-component evidence: call unmodified pinned PC11/PC61/PC26 generators.
# Install the controlled clock before compiling their bare `time` calls; all
# module state and any incidental writes belong to the caller's temporary CWD.
use strict;
use warnings;
no warnings 'once';
use JSON::PP;
use MIME::Base64 qw(encode_base64);

BEGIN {
    die "DXSPIDER_ROOT, DXSPIDER_TEST_STATE, GOCLUSTER_ROOT and DXSPIDER_TEST_AT required\n"
        unless $ENV{DXSPIDER_ROOT} && $ENV{DXSPIDER_TEST_STATE} && $ENV{GOCLUSTER_ROOT}
            && defined $ENV{DXSPIDER_TEST_AT} && $ENV{DXSPIDER_TEST_AT} =~ /\A[0-9]+\z/;
    $main::controlled_time = 0 + $ENV{DXSPIDER_TEST_AT};
    *CORE::GLOBAL::time = sub { $main::controlled_time };
    unshift @INC, "$ENV{DXSPIDER_ROOT}/perl", "$ENV{GOCLUSTER_ROOT}/peer/testdata/dxspider";
    $main::root = $ENV{DXSPIDER_TEST_STATE};
    $main::data = "$main::root/data";
    $main::local_data = "$main::root/local_data";
    $main::system = "$main::root/sys";
    $main::is_win = 1;
    $main::systime = $main::controlled_time;
    $main::systime_daystart = int($main::systime / 86400) * 86400;
    $main::starttime = $main::systime;
    $main::lang = 'en';
    require File::Path;
    File::Path::make_path($main::data, $main::local_data, "$main::local_data/log", "$main::local_data/debug");
}

use DXProt;
$DXDebug::no_stdout = 1;
# Configure the ordinary native hop table; get_hops and all formatters remain
# the pinned implementations. PC26 deliberately has no transport hop.
$DXProt::def_hopcount = 3;
my %frames = (
    PC11 => DXProt::pc11('W1XYZ', 14074.1, 'K1ABC', 'CQ TEST'),
    PC61 => DXProt::pc61('W1XYZ', 14074.1, 'K1ABC', 'CQ TEST', '203.0.113.7'),
    PC26 => DXProt::pc26(14074.1, 'K1ABC', $main::controlled_time, 'CQ TEST', 'W1XYZ', $main::mycall),
);
print JSON::PP->new->canonical->encode({ map { $_ => encode_base64($frames{$_}, '') } keys %frames }), "\n";
