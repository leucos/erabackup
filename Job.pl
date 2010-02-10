#!/usr/bin/perl

# ERASME::Backup::Job.pl
#
# Enqueue backups in bontmia processes
#


package ERASME::Backup::Job;

use strict;
use threads;

use Thread::Running;

use constant TRUE => 1;
use constant FALSE => 1;

my $Census  = 0;

sub new {
	my $class = shift;
	my $id = shift;

	my $self  = {};

	$self->{ID}          = $id;

	$self->{ENABLED}     = TRUE;
	$self->{PRIORITY}    = 100;
	$self->{HOST}        = undef;
	$self->{REMOTEUSER}  = "root";
	$self->{ROTATION}    = undef;
	$self->{SOURCES}      = [];
	$self->{DESTINATION} = undef;

	$self->{_CENSUS} =  \$Census;

	$self->{_CREATE_TIME} = time;
	$self->{_START_TIME}  = undef;
	$self->{_END_TIME}  = undef;

	$self->{_COMMAND} = undef;
	$self->{_RESULT}  = undef;

	$self->{_THREAD_ID}   = undef;

	bless ($self, $class);

	++${$self->{"_CENSUS"}};

  return $self;
}

sub DESTROY {
	my $self = shift;

	++ ${ $self->{"_CENSUS"} };
}

sub population {
	my $self = shift;

	if (ref $self) {
		return ${ $self->{"_CENSUS"} };
	} else {
		return $Census;
	}
}

sub initialize {
	my ($self, $data) = @_;

	$self->enabled($data->{"enabled"}) if defined $data->{"enabled"};
	$self->priority($data->{"priority"}) if defined $data->{"priority"};
	$self->host($data->{"host"});
	$self->remoteuser($data->{"remoteuser"}) if defined $data->{"remoteuser"};
	$self->rotation($data->{"rotation"});
	$self->destination($data->{"destination"});

	if (! ref $data->{"source"}) {
		#not a ref => not an array
		$self->sources($data->{"source"});
	} else {
		$self->sources(@{$data->{"source"}});
	}

#	if (ref $data->{"source"} eq 'SCALAR') {
#		$self->sources([ {$data->{"source"}} ]);
#	} else {
#		$self->sources(@{$data->{"source"}});
#	}
}

##
# Accessors
##

sub id {
	my $self = shift;

	return $self->{ID};
}

sub create_time {
	my $self = shift;

	return $self->{_CREATE_TIME};
}

sub start_time {
	my $self = shift;

	return $self->{_START_TIME};
}

sub end_time {
	my $self = shift;

	return $self->{_END_TIME};
}

sub command {
	my $self = shift;

	return $self->{_COMMAND};
}

sub result {
	my $self = shift;

	return $self->{_RESULT};
}


sub enabled {
	my $self = shift;

	if (@_) { $self->{ENABLED} = shift }
	return $self->{ENABLED};
}

sub dryrun {
	my $self = shift;

	if (@_) { $self->{DRYRUN} = shift }
	return $self->{DRYRUN};
}

sub options {
	my $self = shift;

	if (@_) { $self->{OPTIONS} = shift }
	return $self->{OPTIONS};
}

sub priority {
	my $self = shift;
	if (@_) { $self->{PRIORITY} = shift }
	return $self->{PRIORITY};
}

sub host {
	my $self = shift;
	if (@_) { $self->{HOST} = shift }
	return $self->{HOST};
}

sub remoteuser {
	my $self = shift;
	if (@_) { $self->{REMOTEUSER} = shift }
	return $self->{REMOTEUSER};
}

sub rotation {
	my $self = shift;
	if (@_) { $self->{ROTATION} = shift }
	return $self->{ROTATION};
}

sub destination {
	my $self = shift;
	if (@_) { $self->{DESTINATION} = shift }
	return $self->{DESTINATION};
}

sub sources {
	my $self = shift;
	if (@_) { @{ $self->{SOURCES} } = @_ }
	return @{ $self->{SOURCES} };
}

##
# Thread starting
##

sub exec {
	my $self = shift;

	my $output;

	my $c = $self->{_COMMAND} . " 2>&1 |";

	return "Unable to start job - check shell command" unless open (CMD, $c);
	$output .= $_ while (<CMD>);
	close CMD;

	return $output;
}

sub start {
	my $self = shift;

	if (!$self->{ENABLED}) {
		print STDERR "[backp] Skipping target " . $self->{HOST} . "\n";
		return undef;
	}

	$self->{_COMMAND} = "bontmia ";
	$self->{_COMMAND}.= "--dest " . $self->{DESTINATION} . " ";
	$self->{_COMMAND}.= "--rotation " . $self->{ROTATION} . " ";
  $self->{_COMMAND}.= "--dryrun " if ($self->{DRYRUN});
  $self->{_COMMAND}.= $self->{OPTIONS} . " " if (defined $self->{OPTIONS});


	foreach my $src (@{$self->{SOURCES}}) {
		$self->{_COMMAND}.= $self->{REMOTEUSER} . "@" . $self->{HOST} . ":" . $src . " ";
	}

	print STDERR "[backp] Backuping target " . $self->{HOST} . "\n";
	print STDERR "[bontm] " . $self->{_COMMAND} . "\n";

	$self->{_START_TIME} = time;

#	$self->{_THREAD} = threads->new( sub { sleep rand 5; return ('a'..'z')[rand (26)] } );
	$self->{_THREAD} = threads->new( sub { return $self->exec(); } );

	$self->{_THREAD_ID} = $self->{_THREAD}->tid();

	return $self->{_THREAD_ID};
}

sub end {
	my $self = shift;

	if ($self->is_joinable()) {
		$self->{_END_TIME} = time;
		$self->{_RESULT} = $self->{_THREAD}->join();
		return TRUE;
	} else {
		return FALSE;
	}
}

sub tid {
	my $self = shift;

	return $self->{_THREAD_ID};
}

sub status {
	my $self = shift;

	return "DISABLED" unless ($self->{ENABLED});

	return "READY" if (! defined ($self->{_THREAD_ID}) );
	return "RUNNING" if ($self->is_running);
	return "CLOSING" if ($self->is_joinable);

	return "TERMINATED";
}

sub is_joinable {
	my $self = shift;

	if (defined $self->{_THREAD}) {
		return $self->{_THREAD}->tojoin();
	} else {
		return undef;
	}
}

sub is_running {
	my $self = shift;

	if (defined $self->{_THREAD}) {
		return $self->{_THREAD}->running();
	} else {
		return undef;
	}
}

1;

