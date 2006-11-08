#!/usr/bin/perl

use strict;
use Test::More tests => 12;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;
my $expire;
my $wait_time;
my $msec_granularity = 0;

print $sock "set foo 0 0 6\r\nfooval\r\n";
is(scalar <$sock>, "STORED\r\n", "stored foo");
mem_get_is($sock, "foo", "fooval");

print $sock "set bar 0 0 6\r\nbarval\r\n";
is(scalar <$sock>, "STORED\r\n", "stored foo");
mem_get_is($sock, "bar", "barval");

print $sock "flush_regex bar\r\n";
is(scalar <$sock>, "DELETED\r\n", "did flush_regex");
mem_get_is($sock, "foo", "fooval");
mem_get_is($sock, "bar", undef);

print $sock "set bar 0 0 6\r\nbarval\r\n";
is(scalar <$sock>, "STORED\r\n", "stored foo");
mem_get_is($sock, "bar", "barval");

print $sock "flush_regex f.*\r\n";
is(scalar <$sock>, "DELETED\r\n", "did flush_regex");
mem_get_is($sock, "foo", undef);
mem_get_is($sock, "bar", "barval");

