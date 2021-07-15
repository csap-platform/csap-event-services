/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.csap.events.monitoring ;

import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.mongodb.ReadPreference ;
import com.mongodb.connection.ServerDescription ;
import com.mongodb.event.ClusterClosedEvent ;
import com.mongodb.event.ClusterDescriptionChangedEvent ;
import com.mongodb.event.ClusterListener ;
import com.mongodb.event.ClusterOpeningEvent ;

/**
 *
 * @author pnightin
 */
public class MongoClusterListener implements ClusterListener {

	final Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	private final ReadPreference readPreference ;
	private boolean isWritable ;
	private boolean isReadable ;

	public MongoClusterListener ( final ReadPreference readPreference ) {

		this.readPreference = readPreference ;

	}

	@Override
	public void clusterOpening ( final ClusterOpeningEvent clusterOpeningEvent ) {

		logger.info( "{}",
				clusterOpeningEvent.getClusterId( ) ) ;

	}

	@Override
	public void clusterClosed ( final ClusterClosedEvent clusterClosedEvent ) {

		logger.info( "{}",
				clusterClosedEvent.getClusterId( ) ) ;

	}

	/**
	 * Verbose as timings will change on every iteration. To avoid overloading logs,
	 * use state in class to limit output.
	 *
	 * @param event
	 */
	@Override
	public void clusterDescriptionChanged ( final ClusterDescriptionChangedEvent event ) {

		logger.debug( "{}", event ) ;

		if ( event.getNewDescription( ).getShortDescription( ).length( ) != event.getPreviousDescription( )
				.getShortDescription( ).length( ) ) {

			StringBuilder mongoStatus = new StringBuilder( "\n Status Change:" ) ;

			for ( ServerDescription desc : event.getNewDescription( ).getServerDescriptions( ) ) {

				mongoStatus.append( "\n " + desc.getShortDescription( ) ) ;

			}

			logger.warn( mongoStatus.toString( ) ) ;

		}

	}
}
