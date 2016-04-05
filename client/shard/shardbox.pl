#!/usr/bin/perl -s

use strict;
use warnings;
use IO::Socket::INET;
use vars qw/$s/;

$| = 1; # autoflush
my $iproto_sync = 1;
my $iproto_len = 0;


sub usage {
    my $msg = shift;
    my $name = $0 =~ /([^\/]+)$/ ? $1 : $0;
    print "$msg\n\n" if $msg;
    print <<EOD;
Shard create/alter:
$name -s=<HOST> shard <SID> alter <por|paxos|part> <MASTER> [REPLICA1] [REPLICA2] [REPLICA3] [REPLICA4]

Object space create/drop/truncate:
$name -s=<HOST> shard <SID> obj_space <OID> create [no_snap] [no_wal] <INDEX CONF>
$name -s=<HOST> shard <SID> obj_space <OID> drop
$name -s=<HOST> shard <SID> obj_space <OID> truncate

Index create/drop:
$name -s=<HOST> shard <SID> obj_space <OID> index <IID> create <INDEX CONF>
$name -s=<HOST> shard <SID> obj_space <OID> index <IID> drop

where:
   HOST: addr:port
   SID: 0-65535
   OID: 0-255
   IID: 0-9
   MASTER, REPLICA1-4: peer.name from octopus.cfg
   INDEX CONF: <INDEX TYPE> [unique] <FIELD0 CONF> [, FIELD1 CONF] ... [, FIELD7 CONF]
   INDEX TYPE: hash|numhash|tree|sptree|fasttree|compacttree
   FIELD CONF: <FIELD TYPE> <FID> [desc|asc]
   FID: 0-255
   FIELD TYPE: unum16|snum16|unum32|snum32|unum64|snum64|string
EOD
    exit 1;
}

sub shift_opt {
    my %h = @_;
    foreach (keys %h) {
	if (defined $ARGV[0] and $_ eq $ARGV[0]) {
	    shift @ARGV;
	    return $h{$_};
	}
    }
    return undef;
}

sub shift_cast {
    my %h = @_;
    my $r = shift_opt(@_);
    return $r if defined $r;
    $ARGV[0] = 'nothing' if not defined $ARGV[0];
    usage "expect " . join('|', sort keys %h) . " got $ARGV[0]";
}

sub shift_int {
    my ($max) = @_;
    my $value = $ARGV[0];
    if (not defined $value or $value !~ /^\d+$/ or $value < 0 or $value > $max) {
	$value = 'nothing' if not defined $value;
	usage "expect 0-$max, got $value";
    }
    return shift @ARGV;
}

sub msg_shard {
    my ($sid) = @_;
    my $msg_code = shift_cast('alter' => 0xff02);
    my $type = shift_cast(por => 0, paxos => 1, part => 2);
    my ($master, $replica1, $replica2, $replica3, $replica4) = @ARGV;

    my $version = 0;
    my $tm = 0;
    my $row_count = 0;
    my $run_crc = 0;
    my $mod_name = "Box";

    usage "master not specified" unless $master;
    $replica1 ||= "";
    $replica2 ||= "";
    $replica3 ||= "";
    $replica4 ||= "";

    return pack("SSLL" . "CCQLL" . "a16" x 6,
		$msg_code, $sid, $iproto_len, $iproto_sync,
		$version, $type, $tm, $row_count, $run_crc,
		$mod_name, $master, $replica1, $replica2, $replica3, $replica4);
}


sub index_conf {
    my $version = 0x10;
    my $type = shift_cast(hash => 0, tree => 5,
			  numhash => 1, sptree => 3,
			  fasttree => 3, compacttree => 4,
			  postree => 5);
    my $unique = shift_opt(unique => 1) || 0;

    if ($type == 1 || $type == 2) {
	$unique = 1;
    }

    my @fields;
    do {
	my $ftype = shift_cast(unum16 => 1, snum16 => 2,
			       unum32 => 3, snum32 => 4,
			       unum64 => 5, snum64 => 6,
			       string => 7);
	my $fid = shift_int(255);
	my $order = shift_opt(asc => 1, desc => 0xff) || 1;
	push @fields, $fid, $order, $ftype;
    } while (scalar @ARGV > 0);
    my $cardinality =  @fields / 3;
    usage "too many index fields" if $cardinality > 8;

    pack ('C*',
	  $version, $cardinality, $type, $unique,
	  @fields);
}

sub msg_obj_space {
    my ($sid, $oid) = @_;
    my $msg_code = shift_cast(create => 240, drop => 242, truncate => 244);
    my $flags = 0;

    my $req;
    if ($msg_code == 240) {
        $flags |= (shift_opt(no_snap => 1) ? 0 : 1);
	$flags |= (shift_opt(no_wal  => 2) ? 0 : 2);
	my $cardinality = 0;
	$req = pack("LLC", $oid, $flags, $cardinality) . index_conf();
    } else {
	$req = pack("LL", $oid, $flags);
    }

    return pack("SSLL", $msg_code, $sid, $iproto_len, $iproto_sync) . $req;
}

sub msg_index {
    my ($sid, $oid, $iid) = @_;
    my $msg_code = shift_cast(create => 241, drop => 243);
    my $flags = 0;
    my $index_conf = $msg_code == 241 ? index_conf() : "";
    return pack("SSLL" . "LLC",
		$msg_code, $sid, $iproto_len, $iproto_sync,
		$oid, $flags, $iid) . $index_conf;
}

sub op {
    if (not defined $ARGV[0] or $ARGV[0] !~ /^$_[0]$/) {
	$ARGV[0] = 'nothing' if not defined $ARGV[0];
	usage "expect $_[0], got $ARGV[0]\n";
    }
    return $ARGV[0];
}


usage() unless $s;

my ($sid, $oid, $iid, $req, $resp);
op('shard'); shift @ARGV;
$sid = shift_int(65535);
if (op(qr/alter|obj_space/) ne 'obj_space') {
    $req = msg_shard($sid);
} else {
    shift @ARGV;
    $oid = shift_int(255);
    if (op(qr/create|drop|truncate|index/) ne 'index') {
	$req = msg_obj_space($sid, $oid);
    } else {
	shift @ARGV;
	$iid = shift_int(9);
	$req = msg_index($sid, $oid, $iid);
    }
}


my $sock = new IO::Socket::INET(PeerHost => $s, Proto => 'tcp')or die "connect: $!";
substr($req, 4, 4) = pack('L', length($req) - 12);
$sock->send($req);
$sock->recv($resp, 1024);

my ($msg, $len, $sync, $ret_code) = unpack("LLLL", $resp);
if ($ret_code != 0) {
    printf('error: 0x%x %s'."\n", $ret_code, substr($resp, 16));
} else {
    print "ok\n";
}

exit 0;
use Data::Hexdumper qw(hexdump);
print hexdump($req);
print hexdump($resp);

