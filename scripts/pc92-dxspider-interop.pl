#!/usr/bin/env perl
# Runs the unmodified pinned DXSpider module graph in an isolated site. The only
# adapter is a socket sink: actual Msg/ExtMsg framing, DXProt validation/handlers,
# DXUser storage, and Route objects execute normally. This is receiver-component
# interoperability evidence, not a running production DXSpider daemon.
use strict;
use warnings;
no warnings "once";
use File::Path qw(make_path);
use File::Copy qw(copy);
use File::Spec;
use JSON::PP;

BEGIN {
    die "DXSPIDER_ROOT and DXSPIDER_TEST_STATE required\n"
        unless $ENV{DXSPIDER_ROOT} && $ENV{DXSPIDER_TEST_STATE};
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

package CaptureSocket;
sub new { bless { output => [] }, shift }
sub write { my ($self, $bytes) = @_; push @{$self->{output}}, $bytes; return $self }
sub close_gracefully { }

package main;
my $json = JSON::PP->new->canonical;
$| = 1;
my ($channel, $transport, $sink);

sub setup {
    my ($request) = @_;
    die "receiver already initialized" if $channel;
    my $local_user = DXUser->new($main::mycall);
    $local_user->sort('S'); $local_user->put;
    DXProt::init();
    $main::routeroot = Route::Node->new($main::mycall, $main::version*100+5300, Route::here(1));
    my $call = $request->{call} || 'N0CALL';
    my $user = DXUser->new($call);
    $user->sort($request->{sort} || 'A');
    $user->wantpc9x(defined $request->{wantpc9x} ? $request->{wantpc9x} : 1);
    $user->put;
    $sink = CaptureSocket->new;
    $transport = ExtMsg->new(sub {
        my ($conn, $message) = @_;
        $message =~ s/^I[^|]+\|// or die "unexpected ExtMsg dispatch";
        $channel->normal($message);
    });
    $transport->{sock} = $sink;
    $transport->{peerhost} = '203.0.113.2';
    $transport->{sockhost} = '198.51.100.1';
    $transport->{state} = 'C';
    $transport->{call} = $call;
    $channel = DXProt->new($call, $transport, $user);
    $channel->{outbound} = $request->{outbound} ? 1 : 0;
    $channel->{state} = 'init';
    $channel->{do_pc9x} = 0;
    $channel->{pingave} = 0;
    $channel->sendinit if !$channel->{outbound};
}

sub snapshot {
    my ($calls) = @_;
    my %nodes;
    my %users;
    for my $call (@{$calls || []}, $channel->{call}) {
        my $route = Route::get($call);
        if ($route) {
            $nodes{$call} = { map { $_ => $route->{$_} } grep { exists $route->{$_} } qw(call version build ip flags lastid users nodes parent K) };
        }
        my $user = DXUser::get_current($call);
        if ($user) {
            $users{$call} = { map { $_ => $user->{$_} } grep { exists $user->{$_} } qw(call sort version build K) };
        }
    }
    my @output = map { my $s = $_; $s =~ s/\r?\n$//; $s } @{$sink->{output}};
    $sink->{output} = [];
    return {
        tx => \@output,
        channel => { map { $_ => $channel->{$_} } grep { exists $channel->{$_} } qw(call sort state version build do_pc9x do_pc91) },
        routes => \%nodes, users => \%users,
        node_total => Route::Node::count(), user_total => Route::User::count(),
        received_bytes => $transport->{datain}, received_lines => $transport->{linesin},
    };
}

while (my $line = <STDIN>) {
    my $request = $json->decode($line);
    if (($request->{command} || '') eq 'init') {
        setup($request);
    } elsif (($request->{command} || '') eq 'frame') {
        die "receiver not initialized" unless $channel;
        $main::systime = $request->{at} || time;
        $main::systime_daystart = int($main::systime / 86400) * 86400;
        my $wire = $request->{line} . "\r\n";
        my $chunk = $request->{chunk} || 4096;
        while (length $wire) {
            $transport->_rcv(substr($wire, 0, $chunk, ''));
        }
    } elsif (($request->{command} || '') ne 'snapshot') {
        die "unknown receiver command";
    }
    print $json->encode(snapshot($request->{calls})), "\n";
}
DXUser::sync();
