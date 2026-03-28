package GRNOC::Simp::Poller;

use strict;
use warnings;
use Data::Dumper;
use Moo;
use Types::Standard qw( Str Bool Int );

use Parallel::ForkManager;
use Proc::Daemon;
use Digest::MD5 qw(md5_hex);

use GRNOC::Config;
use GRNOC::Log;

use GRNOC::Simp::Poller::Worker;

### required attributes ###

has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );

has logging_file => ( is => 'ro',
                      isa => Str,
                      required => 1 );

### optional attributes ###

has daemonize => ( is => 'ro',
                   isa => Bool,
                   default => 1 );

# Hash ring: which poller instance is this (0-indexed)
has poller_id => ( is => 'rwp',
                   isa => Int,
                   default => 0 );

# Hash ring: total number of poller instances sharing the load
has total_pollers => ( is => 'rwp',
                       isa => Int,
                       default => 1 );

### private attributes ###

has config => ( is => 'rwp' );

has logger => ( is => 'rwp' );

has children => ( is => 'rwp',
                  default => sub { [] } );

sub BUILD {

    my ( $self ) = @_;

    # create and store logger object
    my $grnoc_log = GRNOC::Log->new( config => $self->logging_file );
    my $logger = GRNOC::Log->get_logger();

    $self->_set_logger( $logger );

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 1 );

    $self->_set_config( $config );

    # read optional hash-ring settings from config
    # <poller id="0" total="3"/>
    my $poller_id     = $config->get( '/config/poller/@id' );
    my $total_pollers = $config->get( '/config/poller/@total' );

    if ( defined $poller_id && defined $poller_id->[0] ) {
        $self->_set_poller_id( int( $poller_id->[0] ) );
    }
    if ( defined $total_pollers && defined $total_pollers->[0] && $total_pollers->[0] > 0 ) {
        $self->_set_total_pollers( int( $total_pollers->[0] ) );
    }

    return $self;
}

# Returns 1 if this poller instance owns the given host IP on the hash ring.
# Uses MD5 of the IP so distribution is deterministic and even across pollers.
sub _host_in_ring {

    my ( $self, $ip ) = @_;

    return 1 if $self->total_pollers <= 1;

    # Take the first 8 hex digits of MD5 (32 bits) and map to a poller slot
    my $hash = hex( substr( md5_hex($ip), 0, 8 ) );
    return ( $hash % $self->total_pollers ) == $self->poller_id;
}


sub start {

    my ( $self ) = @_;

    $self->logger->info( 'Starting.' );

    $self->logger->debug( 'Setting up signal handlers.' );

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( 'Received SIG TERM.' );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( 'Received SIG HUP.' );
    };

    # need to daemonize
    if ( $self->daemonize ) {

        $self->logger->debug( 'Daemonizing.' );

        my $daemon = Proc::Daemon->new( pid_file => $self->config->get( '/config/pid-file' ) );

        my $pid = $daemon->Init();

        # in child/daemon process
        if ( !$pid ) {

            $self->logger->debug( 'Created daemon process.' );

            # change process name
            $0 = "simpPoller";

            $self->_create_workers();
        }
    }

    # dont need to daemonize
    else {

        $self->logger->debug( 'Running in foreground.' );

        $self->_create_workers();
    }

    return 1;
}

sub stop {

    my ( $self ) = @_;

    $self->logger->info( 'Stopping.' );

    my @pids = @{$self->children};

    $self->logger->debug( 'Stopping child worker processes ' . join( ' ', @pids ) . '.' );

    return kill( 'TERM', @pids );
}

#-------- end of multprocess boilerplate
sub _create_workers {

    my ( $self ) = @_;



    #--- get the set of active groups 
    my $groups  = $self->config->get( "/config/group" );

    my $forker = Parallel::ForkManager->new( 10 );  #--- this really should be configurable max
    
    #--- create workers for each group
    foreach my $group (@$groups){
      next if($group->{'active'} == 0);
      my $name    = $group->{"name"};
      my $workers = $group->{'workers'};

      my $poll_interval = $group->{'poll_interval'};

      my @oids;
      foreach my$line(@{$group->{'mib'}}){
	push(@oids,$line->{'oid'});
      }

      #--- filter hosts to only those owned by this poller on the hash ring
      my @ring_hosts = grep { $self->_host_in_ring( $_->{'ip'} ) } @{$group->{'host'}};

      $self->logger->info( "Hash ring (id=" . $self->poller_id . ", total=" . $self->total_pollers . "): "
          . scalar(@ring_hosts) . " of " . scalar(@{$group->{'host'}}) . " hosts assigned to this poller for group: $name" );

      #--- split ring-assigned hosts between workers (round-robin)
      my %hosts;
      my $idx=0;
      foreach my $host (@ring_hosts){
        push(@{$hosts{$idx}},$host);
        $idx++;
        if($idx>=$workers) { $idx = 0; }
      }

      $self->logger->info( "Creating $workers child processes for group: $name" );     

      # keep track of children pids
      $forker->run_on_start( sub {

        my ( $pid ) = @_;

        $self->logger->debug( "Child worker process $pid created." );

        push( @{$self->children}, $pid );
      } );


      # create workers
      for (my $worker_id=0; $worker_id<$workers;$worker_id++) {
        $forker->start() and next;
   
        # create worker in this process
        my $worker = GRNOC::Simp::Poller::Worker->new( worker_name => "$name-$worker_id",
						       config      => $self->config,
 						       oids        => \@oids,
						       hosts 	   => $hosts{$worker_id}, 
                                                       poll_interval => $poll_interval,
                                                       logger      => $self->logger);

        # this should only return if we tell it to stop via TERM signal etc.
        $worker->start();

        # exit child process
        $forker->finish();
      }
 
     


    } 

    $self->logger->debug( 'Waiting for all child worker processes to exit.' );

    # wait for all children to return
    $forker->wait_all_children();

    $self->_set_children( [] );

    $self->logger->debug( 'All child workers have exited.' );
}

1;
