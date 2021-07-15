/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.csap.events.monitoring ;

import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.mongodb.event.CommandFailedEvent ;
import com.mongodb.event.CommandListener ;
import com.mongodb.event.CommandStartedEvent ;
import com.mongodb.event.CommandSucceededEvent ;

/**
 *
 * @author pnightin
 */
public class MongoCommandListener implements CommandListener {

	final Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Override
	public void commandStarted ( final CommandStartedEvent event ) {

		logger.debug( "Sent command '{}:{}' with id {} to database '{}' "
				+ "on connection '{}' to server '{}'",
				event.getCommandName( ),
				event.getCommand( ).get( event.getCommandName( ) ),
				event.getRequestId( ),
				event.getDatabaseName( ),
				event.getConnectionDescription( )
						.getConnectionId( ),
				event.getConnectionDescription( ).getServerAddress( ) ) ;

	}

	@Override
	public void commandSucceeded ( final CommandSucceededEvent event ) {

		logger.debug( "Successfully executed command '{}' with id {} "
				+ "on connection '{}' to server '{}'",
				event.getCommandName( ),
				event.getRequestId( ),
				event.getConnectionDescription( )
						.getConnectionId( ),
				event.getConnectionDescription( ).getServerAddress( ) ) ;

	}

	@Override
	public void commandFailed ( final CommandFailedEvent event ) {

		logger.warn( "Failed execution of command '{}' with id {} "
				+ "on connection '{}' to server '{}' with exception '{}'",
				event.getCommandName( ),
				event.getRequestId( ),
				event.getConnectionDescription( )
						.getConnectionId( ),
				event.getConnectionDescription( ).getServerAddress( ),
				event.getThrowable( ) ) ;

	}
}
