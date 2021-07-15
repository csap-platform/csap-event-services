package org.csap.events.admin ;

import java.io.Serializable ;

import org.bson.BsonDocument ;

import com.mongodb.MongoClientOptions ;
import com.mongodb.ReadPreference ;

public class ClientOptions implements Serializable {
	int connectionTimeout ;
	int connectionsPerHost ;
	String descriptions ;
	int heartbeatConnectionTimeout ;
	int heartbeatFrequency ;
	int heartbeatSocketTimeout ;
	int localThreshhold ;
	int maxConnectionIdleTime ;
	int maxConnectionLifeTime ;
	int maxWaitTime ;
	int minConnectionsPerHost ;
	int minHeartbeatFrequency ;
	ReadPreference readPref ;
	String requiredReplicaSetName ;
	int serverSelectionTimeout ;
	int socketTimeout ;
	int threadsAllowedToBlockForConnectionMultiplier ;
	BsonDocument writeConcern ;
	boolean isSocketKeepAlive ;

	public ClientOptions ( MongoClientOptions options ) {

		connectionsPerHost = options.getConnectionsPerHost( ) ;
		connectionTimeout = options.getConnectTimeout( ) ;
		descriptions = options.getDescription( ) ;
		heartbeatConnectionTimeout = options.getHeartbeatConnectTimeout( ) ;
		heartbeatFrequency = options.getHeartbeatFrequency( ) ;
		heartbeatSocketTimeout = options.getHeartbeatSocketTimeout( ) ;
		localThreshhold = options.getLocalThreshold( ) ;
		maxConnectionIdleTime = options.getMaxConnectionIdleTime( ) ;

		maxConnectionLifeTime = options.getMaxConnectionLifeTime( ) ;
		maxWaitTime = options.getMaxWaitTime( ) ;
		minConnectionsPerHost = options.getMinConnectionsPerHost( ) ;
		minHeartbeatFrequency = options.getMinHeartbeatFrequency( ) ;
		readPref = options.getReadPreference( ) ;
		requiredReplicaSetName = options.getRequiredReplicaSetName( ) ;
		serverSelectionTimeout = options.getServerSelectionTimeout( ) ;
		socketTimeout = options.getSocketTimeout( ) ;
		threadsAllowedToBlockForConnectionMultiplier = options.getThreadsAllowedToBlockForConnectionMultiplier( ) ;
		writeConcern = options.getWriteConcern( ).asDocument( ) ;
		isSocketKeepAlive = options.isSocketKeepAlive( ) ;

	}

	public int getConnectionTimeout ( ) {

		return connectionTimeout ;

	}

	public void setConnectionTimeout ( int connectionTimeout ) {

		this.connectionTimeout = connectionTimeout ;

	}

	public int getConnectionsPerHost ( ) {

		return connectionsPerHost ;

	}

	public void setConnectionsPerHost ( int connectionsPerHost ) {

		this.connectionsPerHost = connectionsPerHost ;

	}

	public String getDescriptions ( ) {

		return descriptions ;

	}

	public void setDescriptions ( String descriptions ) {

		this.descriptions = descriptions ;

	}

	public int getHeartbeatConnectionTimeout ( ) {

		return heartbeatConnectionTimeout ;

	}

	public void setHeartbeatConnectionTimeout ( int heartbeatConnectionTimeout ) {

		this.heartbeatConnectionTimeout = heartbeatConnectionTimeout ;

	}

	public int getHeartbeatFrequency ( ) {

		return heartbeatFrequency ;

	}

	public void setHeartbeatFrequency ( int heartbeatFrequency ) {

		this.heartbeatFrequency = heartbeatFrequency ;

	}

	public int getHeartbeatSocketTimeout ( ) {

		return heartbeatSocketTimeout ;

	}

	public void setHeartbeatSocketTimeout ( int heartbeatSocketTimeout ) {

		this.heartbeatSocketTimeout = heartbeatSocketTimeout ;

	}

	public int getLocalThreshhold ( ) {

		return localThreshhold ;

	}

	public void setLocalThreshhold ( int localThreshhold ) {

		this.localThreshhold = localThreshhold ;

	}

	public int getMaxConnectionIdleTime ( ) {

		return maxConnectionIdleTime ;

	}

	public void setMaxConnectionIdleTime ( int maxConnectionIdleTime ) {

		this.maxConnectionIdleTime = maxConnectionIdleTime ;

	}

	public int getMaxConnectionLifeTime ( ) {

		return maxConnectionLifeTime ;

	}

	public void setMaxConnectionLifeTime ( int maxConnectionLifeTime ) {

		this.maxConnectionLifeTime = maxConnectionLifeTime ;

	}

	public int getMaxWaitTime ( ) {

		return maxWaitTime ;

	}

	public void setMaxWaitTime ( int maxWaitTime ) {

		this.maxWaitTime = maxWaitTime ;

	}

	public int getMinConnectionsPerHost ( ) {

		return minConnectionsPerHost ;

	}

	public void setMinConnectionsPerHost ( int minConnectionsPerHost ) {

		this.minConnectionsPerHost = minConnectionsPerHost ;

	}

	public int getMinHeartbeatFrequency ( ) {

		return minHeartbeatFrequency ;

	}

	public void setMinHeartbeatFrequency ( int minHeartbeatFrequency ) {

		this.minHeartbeatFrequency = minHeartbeatFrequency ;

	}

	public ReadPreference getReadPref ( ) {

		return readPref ;

	}

	public void setReadPref ( ReadPreference readPref ) {

		this.readPref = readPref ;

	}

	public String getRequiredReplicaSetName ( ) {

		return requiredReplicaSetName ;

	}

	public void setRequiredReplicaSetName ( String requiredReplicaSetName ) {

		this.requiredReplicaSetName = requiredReplicaSetName ;

	}

	public int getServerSelectionTimeout ( ) {

		return serverSelectionTimeout ;

	}

	public void setServerSelectionTimeout ( int serverSelectionTimeout ) {

		this.serverSelectionTimeout = serverSelectionTimeout ;

	}

	public int getSocketTimeout ( ) {

		return socketTimeout ;

	}

	public void setSocketTimeout ( int socketTimeout ) {

		this.socketTimeout = socketTimeout ;

	}

	public int getThreadsAllowedToBlockForConnectionMultiplier ( ) {

		return threadsAllowedToBlockForConnectionMultiplier ;

	}

	public void setThreadsAllowedToBlockForConnectionMultiplier ( int threadsAllowedToBlockForConnectionMultiplier ) {

		this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier ;

	}

	public BsonDocument getWriteConcern ( ) {

		return writeConcern ;

	}

	public void setWriteConcern ( BsonDocument writeConcern ) {

		this.writeConcern = writeConcern ;

	}

	public boolean isSocketKeepAlive ( ) {

		return isSocketKeepAlive ;

	}

	public void setSocketKeepAlive ( boolean isSocketKeepAlive ) {

		this.isSocketKeepAlive = isSocketKeepAlive ;

	}

}
