package org.csap.events.util ;

import java.util.ArrayList ;
import java.util.LinkedHashMap ;
import java.util.List ;
import java.util.Map ;

import org.bson.Document ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class GraphData {
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;
	private Map<String, List<Long[]>> usersGraphData = new LinkedHashMap<>( ) ;
	private Map<String, List<Long[]>> userActivityGraphData = new LinkedHashMap<>( ) ;
	private Map<String, List<Long[]>> servicesGraphData = new LinkedHashMap<>( ) ;
	private Map<String, List<Long[]>> instancesGraphData = new LinkedHashMap<>( ) ;
	private Map<String, List<Long[]>> resource_30_totalGraphData = new LinkedHashMap<>( ) ;
	private Map<String, List<Long[]>> hostsGraphData = new LinkedHashMap<>( ) ;
	private Map<String, List<Long[]>> cpusGraphData = new LinkedHashMap<>( ) ;

	public void addGraphPoint ( Object obj , long dateAsLong ) {

		// logger.debug("adding graph point...");
		Document dbObject = (Document) obj ;
		String key = (String) dbObject.get( "projectName" ) ;
		addToUserGraphData( key, createGraphDataPoint( dbObject, dateAsLong, "activeUsers" ) ) ;
		addToUserActivityGraphData( key, createGraphDataPoint( dbObject, dateAsLong, "totalActivity" ) ) ;
		addToHostsGraphData( key, createGraphDataPoint( dbObject, dateAsLong, "vms" ) ) ;
		addToCpusGraphData( key, createGraphDataPoint( dbObject, dateAsLong, "cpuCount" ) ) ;
		addToServicesGraphData( key, createGraphDataPoint( dbObject, dateAsLong, "serviceCount" ) ) ;
		addToInstancesGraphData( key, createGraphDataPoint( dbObject, dateAsLong, "instanceCount" ) ) ;
		addToResource_30_totalGraphData( key, createGraphDataPoint( dbObject, dateAsLong, "totalLoad" ) ) ;

	}

	private Long[] createGraphDataPoint ( Document dbObject , long dateAsLong , String attributeName ) {

		Long[] graphDataPoint = {} ;

		try {

			String attributeValue = "" + dbObject.get( attributeName ) ;

			if ( attributeValue.contains( "." ) ) {

				attributeValue = attributeValue.split( "\\." )[0] ;

			}

			graphDataPoint = new Long[] {
					dateAsLong, Long.parseLong( attributeValue )
			} ;

		} catch ( Exception e ) {

			logger.error( "Exception while creating graph data point for " + attributeName, e ) ;

		}

		return graphDataPoint ;

	}

	private void addToUserGraphData ( String key , Long[] value ) {

		if ( null == usersGraphData.get( key ) ) {

			List<Long[]> list = new ArrayList<>( ) ;
			usersGraphData.put( key, list ) ;

		}

		usersGraphData.get( key ).add( value ) ;

	}

	private void addToUserActivityGraphData ( String key , Long[] value ) {

		if ( null == userActivityGraphData.get( key ) ) {

			List<Long[]> list = new ArrayList<>( ) ;
			userActivityGraphData.put( key, list ) ;

		}

		userActivityGraphData.get( key ).add( value ) ;

	}

	private void addToServicesGraphData ( String key , Long[] value ) {

		if ( null == servicesGraphData.get( key ) ) {

			List<Long[]> list = new ArrayList<>( ) ;
			servicesGraphData.put( key, list ) ;

		}

		servicesGraphData.get( key ).add( value ) ;

	}

	private void addToInstancesGraphData ( String key , Long[] value ) {

		if ( null == instancesGraphData.get( key ) ) {

			List<Long[]> list = new ArrayList<>( ) ;
			instancesGraphData.put( key, list ) ;

		}

		instancesGraphData.get( key ).add( value ) ;

	}

	private void addToResource_30_totalGraphData ( String key , Long[] value ) {

		if ( null == resource_30_totalGraphData.get( key ) ) {

			List<Long[]> list = new ArrayList<>( ) ;
			resource_30_totalGraphData.put( key, list ) ;

		}

		resource_30_totalGraphData.get( key ).add( value ) ;

	}

	private void addToHostsGraphData ( String key , Long[] value ) {

		if ( null == hostsGraphData.get( key ) ) {

			List<Long[]> list = new ArrayList<>( ) ;
			hostsGraphData.put( key, list ) ;

		}

		hostsGraphData.get( key ).add( value ) ;

	}

	private void addToCpusGraphData ( String key , Long[] value ) {

		if ( null == cpusGraphData.get( key ) ) {

			List<Long[]> list = new ArrayList<>( ) ;
			cpusGraphData.put( key, list ) ;

		}

		cpusGraphData.get( key ).add( value ) ;

	}

	public Map<String, List<Long[]>> getUsersGraphData ( ) {

		return usersGraphData ;

	}

	public void setUsersGraphData ( Map<String, List<Long[]>> usersGraphData ) {

		this.usersGraphData = usersGraphData ;

	}

	public Map<String, List<Long[]>> getUserActivityGraphData ( ) {

		return userActivityGraphData ;

	}

	public void setUserActivityGraphData (
											Map<String, List<Long[]>> userActivityGraphData ) {

		this.userActivityGraphData = userActivityGraphData ;

	}

	public Map<String, List<Long[]>> getServicesGraphData ( ) {

		return servicesGraphData ;

	}

	public void setServicesGraphData ( Map<String, List<Long[]>> servicesGraphData ) {

		this.servicesGraphData = servicesGraphData ;

	}

	public Map<String, List<Long[]>> getInstancesGraphData ( ) {

		return instancesGraphData ;

	}

	public void setInstancesGraphData ( Map<String, List<Long[]>> instancesGraphData ) {

		this.instancesGraphData = instancesGraphData ;

	}

	public Map<String, List<Long[]>> getResource_30_totalGraphData ( ) {

		return resource_30_totalGraphData ;

	}

	public void setResource_30_totalGraphData (
												Map<String, List<Long[]>> resource_30_totalGraphData ) {

		this.resource_30_totalGraphData = resource_30_totalGraphData ;

	}

	public Map<String, List<Long[]>> getHostsGraphData ( ) {

		return hostsGraphData ;

	}

	public void setHostsGraphData ( Map<String, List<Long[]>> hostsGraphData ) {

		this.hostsGraphData = hostsGraphData ;

	}

	public Map<String, List<Long[]>> getCpusGraphData ( ) {

		return cpusGraphData ;

	}

	public void setCpusGraphData ( Map<String, List<Long[]>> cpusGraphData ) {

		this.cpusGraphData = cpusGraphData ;

	}

}
