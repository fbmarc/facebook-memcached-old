#!/usr/bin/perl

use strict;
use Test::More tests => 43;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;

# set foo (and should get it)
print $sock "set foo 0 0 6\r\nfooval\r\n";
is(scalar <$sock>, "STORED\r\n", "stored foo");
mem_get_is($sock, "foo", "fooval");

my $usock = $server->new_udp_sock
    or die "Can't bind : $@\n";

# test all the steps, one by one:
test_single($usock);

# testing sequence numbers
for my $offt (1, 1, 2) {
    my $seq = 160 + $offt;
    my $res = send_udp_request($usock, $seq, "get foo\r\n");
    ok($res, "got result");
    is(keys %$res, 1, "one key (one packet)");
    ok($res->{0}, "only got seq number 0");
    is(substr($res->{0}, 8), "VALUE foo 0 6\r\nfooval\r\nEND\r\n");
    is(hexify(substr($res->{0}, 0, 2)), hexify(pack("n", $seq)), "sequence number in response ($seq) is correct");
}

# testing non-existent stuff
my $res = send_udp_request($usock, 404, "get notexist\r\n");
ok($res, "got result");
is(keys %$res, 1, "one key (one packet)");
ok($res->{0}, "only got seq number 0");
is(hexify(substr($res->{0}, 0, 2)), hexify(pack("n", 404)), "sequence number 404 correct");
is(substr($res->{0}, 8), "END\r\n");

# test multi-packet response with a big value
{
    my $big = "abcd" x 1024;
    my $len = length $big;
    print $sock "set big 0 0 $len\r\n$big\r\n";
    is(scalar <$sock>, "STORED\r\n", "stored big");
    mem_get_is($sock, "big", $big, "big value matches");
    my $res = send_udp_request($usock, 999, "get big\r\n");
    is(scalar keys %$res, 3, "three packet response");
    like(substr($res->{0}, 8), qr/^VALUE big 0 4096/, "first packet has value line");
    is(hexify(substr($res->{1}, 0, 2)), hexify(pack("n", 999)), "sequence number of middle packet is correct");
    is(hexify(substr($res->{0}, 6, 2)), "0008", "response offset of first packet points to start of payload");
    is(hexify(substr($res->{1}, 6, 2)), "0000", "response offset of middle packet is zero since it is all data");
    my ($resid, $seq, $this_numpkts, $offset) = unpack("nnnn", substr($res->{2}, 0, 8));
    is(substr($res->{2}, $offset), "END\r\n", "offset of last packet points to END");
}

# test multi-packet response with several small values, to make sure
# value-offset field is correct
{
    my $big = "abcd" x 100;
    my $len = length $big;
    print $sock "set big 0 0 $len\r\n$big\r\n";
    is(scalar <$sock>, "STORED\r\n", "stored big");
    mem_get_is($sock, "big", $big, "big value matches");
    my $multi = " big" x 6;
    my $res = send_udp_request($usock, 999, "get$multi\r\n");
    is(scalar keys %$res, 2, "three packet response");
    like(substr($res->{0}, 8), qr/^VALUE big 0 400/, "first packet has value line");
    is(hexify(substr($res->{1}, 0, 2)), hexify(pack("n", 999)), "sequence number of middle packet is correct");
    is(hexify(substr($res->{0}, 6, 2)), "0008", "response offset of first packet points to start of payload");
    my ($resid, $seq, $this_numpkts, $offset) = unpack("nnnn", substr($res->{1}, 0, 8));
    like(substr($res->{1}, $offset), qr/VALUE big 0 400/, "offset of middle packet points to VALUE line");
    is(hexify(substr($res->{1}, 6, 2)), "0124", "response offset of middle packet points to first VALUE line");
}

sub test_single {
    my $usock = shift;
    my $req = pack("nnnn", 45, 0, 1, 0);  # request id (opaque), seq num, #packets, reserved (must be 0)
    $req .= "get foo\r\n";
    ok(defined send($usock, $req, 0), "sent request");

    my $rin = '';
    vec($rin, fileno($usock), 1) = 1;
    my $rout;
    ok(select($rout = $rin, undef, undef, 2.0), "got readability");

    my $sender;
    my $res;
    $sender = $usock->recv($res, 1500, 0);

    my $id = pack("n", 45);
    is(hexify(substr($res, 0, 8)), hexify($id) . '0000' . '0001' . '0008', "header is correct");
    is(length $res, 36, '');
    is(substr($res, 8), "VALUE foo 0 6\r\nfooval\r\nEND\r\n", "payload is as expected");
}

sub hexify {
    my $val = shift;
    $val =~ s/(.)/sprintf("%02x", ord($1))/egs;
    return $val;
}

# returns undef on select timeout, or hashref of "seqnum" -> payload (including headers)
sub send_udp_request {
    my ($sock, $reqid, $req) = @_;

    my $pkt = pack("nnnn", $reqid, 0, 1, 0);  # request id (opaque), seq num, #packets, reserved (must be 0)
    $pkt .= $req;
    my $fail = sub {
        my $msg = shift;
        warn "  FAILING send_udp because: $msg\n";
        return undef;
    };
    return $fail->("send") unless send($sock, $pkt, 0);

    my $ret = {};

    my $got = 0;   # packets got
    my $numpkts = undef;

    while (!defined($numpkts) || $got < $numpkts) {
        my $rin = '';
        vec($rin, fileno($sock), 1) = 1;
        my $rout;
        return $fail->("timeout after $got packets") unless
            select($rout = $rin, undef, undef, 1.5);

        my $res;
        my $sender = $sock->recv($res, 1500, 0);
        my ($resid, $seq, $this_numpkts, $offset) = unpack("nnnn", substr($res, 0, 8));
        die "Response ID of $resid doesn't match request if of $reqid" unless $resid == $reqid;
	die "Offset to first response out of bounds" if ($offset < 8 && $offset != 0) || $offset > length($res);
        die "num packets changed midstream!" if defined $numpkts && $this_numpkts != $numpkts;
        $numpkts = $this_numpkts;
        $ret->{$seq} = $res;
        $got++;
    }
    return $ret;
}

__END__
$sender = recv($usock, $ans, 1050, 0);

__END__
$usock->send


    ($hispaddr = recv(SOCKET, $rtime, 4, 0))        || die "recv: $!";
($port, $hisiaddr) = sockaddr_in($hispaddr);
$host = gethostbyaddr($hisiaddr, AF_INET);
$histime = unpack("N", $rtime) - $SECS_of_70_YEARS ;
