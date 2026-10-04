#!/usr/bin/env perl
# Isolated receiver-component evidence for peer spot relay. Only the socket
# sink and clock input are adapted; the pinned framing, admission, delayed-PC11
# process, Spot::add_local cache and on-disk spot log run without replacement.
use strict;
use warnings;
no warnings "once";
use File::Path qw(make_path);
use File::Copy qw(copy);
use JSON::PP;
use MIME::Base64 qw(decode_base64 encode_base64);

BEGIN {
    die "DXSPIDER_ROOT, DXSPIDER_TEST_STATE and GOCLUSTER_ROOT required\n"
        unless $ENV{DXSPIDER_ROOT} && $ENV{DXSPIDER_TEST_STATE} && $ENV{GOCLUSTER_ROOT};
    unshift @INC, "$ENV{DXSPIDER_ROOT}/perl", "$ENV{GOCLUSTER_ROOT}/peer/testdata/dxspider";
    $main::root = $ENV{DXSPIDER_TEST_STATE};
    $main::data = "$main::root/data";
    $main::local_data = "$main::root/local_data";
    $main::system = "$main::root/sys";
    $main::is_win = 1;
    $main::systime = time;
    $main::systime_daystart = int($main::systime / 86400) * 86400;
    $main::starttime = $main::systime;
    $main::lang = 'en';
    $main::version = 1.57;
    $main::build = 633;
    $main::gitbranch = 'mojo';
    $main::gitversion = '3e9b362';
    $main::clusteraddr = '198.51.100.1';
}

use DXProt;
use ExtMsg;
use DXMsg;
use Route::User;
$DXDebug::no_stdout = 1;
make_path($main::data, $main::local_data, "$main::local_data/log", "$main::local_data/debug");
copy("$ENV{DXSPIDER_ROOT}/data/prefix_data.pl", "$main::data/prefix_data.pl") or die "copy prefix data: $!";
my $prefix_error = Prefix::init();
die $prefix_error if $prefix_error;
DXUser::init(1);
DXDupe::init();
Spot::init();

package SpotCaptureSocket;
sub new { bless { output => [] }, shift }
sub write { my ($self, $bytes) = @_; push @{$self->{output}}, $bytes; return $self }
sub close_gracefully { }

package main;
my $json = JSON::PP->new->canonical;
$| = 1;
my ($channel, $transport, $sink);
my @received;

sub setup {
    my ($request) = @_;
    die "receiver already initialized" if $channel;
    my $local_user = DXUser->new($main::mycall);
    $local_user->sort('S'); $local_user->put;
    DXProt::init();
    $main::routeroot = Route::Node->new($main::mycall, $main::version*100+5300, Route::here(1));
    my $call = 'N0CALL';
    my $user = DXUser->new($call);
    $user->sort('A'); $user->wantpc9x($request->{pc9x} ? 1 : 0); $user->put;
    $sink = SpotCaptureSocket->new;
    $transport = ExtMsg->new(sub {
        my ($conn, $message) = @_;
        $message =~ s/^I[^|]+\|// or die "unexpected ExtMsg dispatch";
        push @received, encode_base64($message, '');
        $channel->normal($message);
    });
    $transport->{sock} = $sink;
    $transport->{peerhost} = '203.0.113.2';
    $transport->{sockhost} = '198.51.100.1';
    $transport->{state} = 'C';
    $transport->{call} = $call;
    $channel = DXProt->new($call, $transport, $user);
    $channel->{outbound} = 1;
    $channel->{state} = 'init';
    $channel->{do_pc9x} = 0;
    $channel->{pingave} = 0;
}

sub snapshot {
    my @spots;
    for my $day (sort keys %Spot::spotcache) {
        for my $spot (@{$Spot::spotcache{$day}}) {
            push @spots, [map { encode_base64(defined $_ ? $_ : '', '') } @$spot];
        }
    }
    my $disk = '';
    if ($Spot::fp->{fh}) {
        $Spot::fp->{fh}->flush() or die "flush receiver spot log: $!";
        open my $file, '<:raw', $Spot::fp->{fn} or die "read receiver spot log: $!";
        local $/;
        $disk = <$file>;
        close $file or die "close receiver spot log: $!";
    }
    return {
        state => $channel->{state}, pc9x => $channel->{do_pc9x} ? JSON::PP::true : JSON::PP::false,
        spots_base64 => \@spots, disk_base64 => encode_base64($disk, ''),
        received_base64 => \@received, total_spots => $Spot::totalspots,
        received_bytes => $transport->{datain}, received_lines => $transport->{linesin},
    };
}

while (my $line = <STDIN>) {
    my $request = $json->decode($line);
    $main::systime = $request->{at} || time;
    $main::systime_daystart = int($main::systime / 86400) * 86400;
    if (($request->{command} || '') eq 'init') {
        setup($request);
    } elsif (($request->{command} || '') eq 'frame') {
        die "receiver not initialized" unless $channel;
        # Decode bytes, never a JSON Unicode sentence. These are exactly the
        # Go writer's bytes, including CRLF; do not add or rewrite framing.
        my $wire = decode_base64($request->{wire_base64});
        my $chunk = $request->{chunk} || 97;
        while (length $wire) {
            $transport->_rcv(substr($wire, 0, $chunk, ''));
        }
    } elsif (($request->{command} || '') eq 'tick') {
        die "receiver not initialized" unless $channel;
        DXProt::process();
    } elsif (($request->{command} || '') ne 'snapshot') {
        die "unknown receiver command";
    }
    print $json->encode(snapshot()), "\n";
}
DXUser::sync();
DXLog::Logclose();
