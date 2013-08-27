#! perl -w
# monitor db filegroup and table size/rows

use strict;
use feature 'say';
use Net::SMTP;
use Win32::DriveInfo;
use Number::Bytes::Human 'format_bytes';
use DBI;
use Config::Simple;

my $cfg = new Config::Simple('dbsizemon.conf')
		or die Config::Simple->error();
		
my $db = $cfg->get_block('DB');
my $mail = $cfg->get_block('mail');
my $opts = $cfg->get_block('options');
my $email_flag = $opts->{enable_mail};
my $dbh = init_handle($db);

my $query1 = "
	select distinct d.name, d.physical_name, d.size  from sys.database_files d, sys.sysindexes i 
	where 
	d.data_space_id = i.groupid
	and objectproperty(i.id,'IsUserTable') = 1 
"; 
my $dbfiles_aref = $dbh->selectall_arrayref($query1);

my %report_hash = %{read_report()};
my $report_log = '';
my $email_report = sprintf("%-15s%-35s%-10s\t%-10s\t%-10s\t%-10s\t%-10s%-10s\t%s\n",
		'type','name', 'total rows', 'total size', 'row delta', 'size delta', 'avg. row delta', 'avg. size delta', 'samples');

my %disks;
my ($total_rows, $total_size) = (0,0);
for my $dbfile_aref (@$dbfiles_aref) {
	my ($fg_name, $db_path, $fg_size) = @$dbfile_aref;
	
	say "filegroup $fg_name";
	
	$fg_size = 0 unless $fg_size;
	
	my ($drive_letter) = ($db_path =~ m/^(\w):/); 
	unless ($disks{$drive_letter}) {
		my (undef, undef, undef, undef, undef, $total, $free) = Win32::DriveInfo::DriveSpace($drive_letter);
		$disks{$drive_letter} = [$total, $total - $free];
	}
	
	my $query2 = "
		select distinct OBJECT_Name(i.id) from sys.filegroups f, sys.sysindexes i, sys.database_files d
		where f.data_space_id = i.groupid
		and d.data_space_id = f.data_space_id
		and d.name = '$fg_name'
		and objectproperty(i.id,'IsUserTable') = 1 
	";
	
	my $tables_aref = $dbh->selectall_arrayref($query2);
	
	my $fg_rowcount  = 0;
	for my $table_aref (@$tables_aref) {
		my ($table) = @$table_aref;
		say "\ttable $table";
		my $query3 = "
			exec sp_spaceused $table
		";
		my (undef, $row_count, undef, $table_size) = $dbh->selectrow_array($query3);
		$row_count = 0 unless $row_count;
		$table_size = 0 unless $table_size;
		$table_size =~ s/\D//g;
		
		calc_metrics("${fg_name}_$table", $table, $row_count, $table_size, 'table');
		$fg_rowcount += $row_count if $row_count;
	}
	
	$total_size += $fg_size;
	$total_rows += $fg_rowcount;
	calc_metrics($fg_name, $fg_name, $fg_rowcount, $fg_size, 'filegroup');
}

calc_metrics('total', 'total', $total_rows, $total_size, "\nall");

for my $disk (keys %disks) {
	my ($size, $used) = @{$disks{$disk}};
	$email_report .= sprintf("Disks\n%s:\tfree: %s\tused: %s\n",$disk, format_bytes($size), format_bytes($used));
}

open (REP, '>report.log');
print REP $report_log;
close REP;

if ($email_flag) {
	
	my @recips = $mail->{recips};
	
	my $smtp_server = $mail->{smtp_server};

	send_email(
		{  smtp_server => $smtp_server,
		   send_to     => \@recips,
		   msg_subject => "$db->{name} Report",
		   msg_body    => $email_report,
		   sender => 'dbsizemon',
		}
	) or die "failed to send mail\n";
}
else {
	print $email_report;
}

sub send_email {
	my $opts_href = shift;

	my $smtp = Net::SMTP->new( $opts_href->{smtp_server} )
		or die "could not connect";

	$smtp->mail( $ENV{USER} )
		or warn "authentication failed using $ENV{USER}\n" and return;

	my @add_list = @{ $opts_href->{send_to} };

	foreach my $add (@add_list) {
		$smtp->to($add) or warn "failed to add recip $add\n";
	}

	$smtp->data();
	
	foreach my $add (@add_list) {
		$smtp->datasend("To: $add\n") or warn "failed to add recip $add\n";
	}

	$smtp->datasend("From: ${\$opts_href->{sender}}\n");
	$smtp->datasend("Subject: ${\$opts_href->{msg_subject}}\n");
	$smtp->datasend("\n");

	$smtp->datasend( $opts_href->{msg_body} )
		or warn "could not write body ${\$opts_href->{msg_body}}\n"
		and return;
		
	$smtp->dataend();

	$smtp->quit;
	
	return 1;
}


sub init_handle {
	my $db = shift;

	return
		DBI->connect(
		sprintf(
			"dbi:ODBC:Database=%s;Driver={SQL Server};Server=%s;UID=%s;PWD=%s",
			$db->{name} || 'master', $db->{server},
			$db->{user}, $db->{pwd}
		)
		) or die "failed to initialize database handle\n", $DBI::errstr;
}

sub read_report {
	open(REP, '<report.log') or return {};
	my %report_hash;
	while(<REP>) {
		chomp;
		my ($name, $row_count, $size, $avg_rowcount, $avg_size, $points) = split "\t", $_, -1;
		$report_hash{$name} = [$row_count, $size, $avg_rowcount, $avg_size, $points];
	}
	close REP;
	return \%report_hash;
}

sub calc_metrics {
	my ($key,$name,$row_count,$size,$type) = @_;
	
	my ($last_rowcount, $last_size, $avg_rowcount, $avg_size, $points,$row_count_delta,$size_delta);
		if (my $last_aref = $report_hash{$key}) {
			($last_rowcount, $last_size, $avg_rowcount, $avg_size, $points) = @{$last_aref};
			$row_count_delta = $row_count - $last_rowcount;
			$size_delta = $size - $last_size;
			$avg_rowcount = int(($avg_rowcount * $points + $row_count_delta) / $points);
			$avg_size = int(($avg_size * $points + $size_delta) / $points);	
		}
		else {
			$avg_rowcount = 0;
			$avg_size = 0;
			$points = 0;
			$row_count_delta = 0;
			$size_delta = 0;
		}
		$points++;
		$report_log .= sprintf("%s\t%u\t%u\t%d\t%d\t%u\n", $key, $row_count, $size, $avg_rowcount, $avg_size, $points);
		$email_report .= sprintf("%-15s%-35s%-10s\t%-10s\t%-10s%-10s\t%-10s\t%-10s\t%u\n", 
				$type,$name, $row_count, format_bytes($size * 8192), $row_count_delta, format_bytes( $size_delta * 8192), $avg_rowcount, format_bytes($avg_size * 8192), $points);

}