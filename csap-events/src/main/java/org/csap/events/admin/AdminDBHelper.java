package org.csap.events.admin ;

import java.util.List ;

import javax.inject.Inject ;

import org.bson.Document ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.mongodb.BasicDBList ;
import com.mongodb.BasicDBObject ;
import com.mongodb.CommandResult ;
import com.mongodb.MongoClient ;
import com.mongodb.MongoClientOptions ;
import com.mongodb.ReadPreference ;
import com.mongodb.ReplicaSetStatus ;
import com.mongodb.ServerAddress ;
import com.mongodb.client.MongoIterable ;

public class AdminDBHelper {
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;
	@Inject
	private MongoClient mongoClient ;

	public MongoIterable<String> getDBNames ( ) {

		MongoIterable<String> dbNames = mongoClient.listDatabaseNames( ) ;
		return dbNames ;

	}

	public MongoIterable<String> getCollectionNames ( String dbname ) {

		MongoIterable<String> colNames = mongoClient.getDatabase( dbname ).listCollectionNames( ) ;
		return colNames ;

	}

	public Document getDbStatsFromPrimary ( String dbname ) {

		Document doc = null ;

		try {

			BasicDBObject command = new BasicDBObject( ) ;
			command.put( "dbStats", 1 ) ;
			command.put( "scale", 1024 ) ;
			doc = mongoClient.getDatabase( dbname ).runCommand( command, ReadPreference.primary( ) ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while getting db error stats", e ) ;

		}

		return doc ;

	}

	public Document getCollectionStatsFromPrimary ( String dbname , String collectionName ) {

		Document doc = null ;

		try {

			BasicDBObject command = new BasicDBObject( ) ;
			command.put( "collStats", collectionName ) ;
			command.put( "scale", 1024 ) ;
			command.put( "verbose", true ) ;
			doc = mongoClient.getDatabase( dbname ).runCommand( command, ReadPreference.primary( ) ) ;

		} catch ( Exception e ) {

			logger.error( "Exception ehile getting primary collection stats", e ) ;

		}

		return doc ;

	}

	public Document getDbStatsFromSecondary ( String dbname ) {

		Document doc = null ;

		try {

			BasicDBObject command = new BasicDBObject( ) ;
			command.put( "dbStats", 1 ) ;
			command.put( "scale", 1024 ) ;
			doc = mongoClient.getDatabase( dbname ).runCommand( command, ReadPreference.secondary( ) ) ;
			logger.debug( "document {} ", doc ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while getting db stats from secondary", e ) ;

		}

		return doc ;

	}

	public Document getCollectionStatsFromSecondary ( String dbname , String collectionName ) {

		Document doc = null ;

		try {

			BasicDBObject command = new BasicDBObject( ) ;
			command.put( "collStats", collectionName ) ;
			command.put( "scale", 1024 ) ;
			command.put( "verbose", true ) ;
			doc = mongoClient.getDatabase( dbname ).runCommand( command, ReadPreference.secondary( ) ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while getting collection stats from secondary", e ) ;

		}

		return doc ;

	}

	public String getDriverVersion ( ) {

		return "Not available" ;// mongoClient.getVersion();

	}

	public MongoClientOptions getMongoOptions ( ) {

		return mongoClient.getMongoClientOptions( ) ;

	}

	public String replicaSetStepDown ( ) {

		String message = "success" ;

		try {

			BasicDBObject command = new BasicDBObject( ) ;
			command.put( "replSetStepDown", 20 ) ;
			Document result = mongoClient.getDatabase( "admin" ).runCommand( command, ReadPreference.primary( ) ) ;

			if ( null != result ) {

				logger.info( result.toString( ) ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while stepping down", e ) ;
			message = "failure" ;

		}

		return message ;

	}

	public CommandResult getReplicaSetStatus ( ) {

		CommandResult result = mongoClient.getDB( "admin" ).command( "replSetGetStatus" ) ;
		return result ;

	}

	public boolean isMongoHealthy ( ) {

		boolean isHealthy = true ;
		CommandResult result = getReplicaSetStatus( ) ;
		BasicDBList dbList = (BasicDBList) result.get( "members" ) ;

		if ( null != dbList ) {

			for ( Object obj : dbList ) {

				BasicDBObject dbObject = (BasicDBObject) obj ;

				if ( dbObject.getInt( "health" ) == 0 ) {

					isHealthy = false ;

				}

			}

		} else {

			isHealthy = false ;

		}

		return isHealthy ;

	}

	public String getPrimary ( ) {

		List<ServerAddress> addressList = mongoClient.getAllAddress( ) ;
		ReplicaSetStatus rsStatus = mongoClient.getReplicaSetStatus( ) ;
		String masterHost = rsStatus.getMaster( ).getHost( ) ;
		int masterPort = rsStatus.getMaster( ).getPort( ) ;

		for ( ServerAddress sAddress : addressList ) {

			int port = sAddress.getPort( ) ;
			String host = sAddress.getHost( ) ;
			if ( host.indexOf( masterHost ) > -1 && masterPort == port )
				return sAddress.getHost( ) ;

		}

		return null ;

	}

	public String getSecondary ( ) {

		List<ServerAddress> addressList = mongoClient.getAllAddress( ) ;
		ReplicaSetStatus rsStatus = mongoClient.getReplicaSetStatus( ) ;
		String masterHost = rsStatus.getMaster( ).getHost( ) ;

		for ( ServerAddress sAddress : addressList ) {

			String host = sAddress.getHost( ) ;
			if ( host.indexOf( masterHost ) == -1 )
				return sAddress.getHost( ) ;

		}

		return null ;

	}

}
