while(<>) {
	if (/INSERT:.*rps:\s+(\d+)/) {
		print "0:$1\n";
	} elsif (/SELECT:.*rps:\s+(\d+)/) {
		print "1:$1\n";
	} elsif (/DELETE:.*rps:\s+(\d+)/) {
		print "2:$1\n";
	}
}
