package org.csap.events.db ;

import static com.mongodb.client.model.Filters.and ;
import static com.mongodb.client.model.Filters.eq ;
import static com.mongodb.client.model.Filters.gte ;
import static com.mongodb.client.model.Filters.lte ;
import static com.mongodb.client.model.Filters.regex ;
import static com.mongodb.client.model.Projections.exclude ;
import static com.mongodb.client.model.Projections.excludeId ;
import static com.mongodb.client.model.Projections.fields ;
import static com.mongodb.client.model.Projections.include ;
import static com.mongodb.client.model.Sorts.descending ;
import static org.csap.events.EventJsonConstants.APPID ;
import static org.csap.events.EventJsonConstants.CREATED_ON ;
import static org.csap.events.EventJsonConstants.LAST_UPDATED_ON ;
import static org.csap.events.EventJsonConstants.LIFE ;

import java.util.ArrayList ;
import java.util.Arrays ;
import java.util.Calendar ;
import java.util.HashMap ;
import java.util.List ;
import java.util.Map ;
import java.util.TimeZone ;
import java.util.concurrent.TimeUnit ;
import java.util.regex.Pattern ;

import javax.inject.Inject ;

import org.apache.commons.lang3.StringUtils ;
import org.bson.Document ;
import org.bson.conversions.Bson ;
import org.bson.types.ObjectId ;
import org.csap.events.EventJsonConstants ;
import org.csap.events.EventMetaData ;
import org.csap.events.util.DateUtil ;
import org.csap.helpers.CSAP ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.cache.annotation.Cacheable ;
import org.springframework.stereotype.Service ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ArrayNode ;
import com.mongodb.BasicDBObject ;
import com.mongodb.MongoClient ;
import com.mongodb.ReadPreference ;
import com.mongodb.client.AggregateIterable ;
import com.mongodb.client.FindIterable ;
import com.mongodb.client.MongoCollection ;
import com.mongodb.client.model.CountOptions ;
import com.mongodb.util.JSON ;

@Service
public class EventDataReader {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Inject
	private MongoClient mongoClient ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Inject
	private EventDataHelper eventDataHelper ;

	// @Cacheable( value = "oneMinuteCache", key = "{'eventListing' +
	// #searchString + #searchString + #searchString}" )
	public FindIterable<Document> getEventsByCriteria ( String searchString , int numRecords , int startIndex ) {

		FindIterable<Document> eventRecords = eventDataHelper.getMongoEventCollection( )
				.find( eventDataHelper.convertUserInterfaceQueryToMongoFilter( searchString ) )
				.sort( descending( getSortString( searchString ) ) )
				.projection( exclude( getExcludedKeys( searchString ) ) )
				.skip( startIndex )
				.limit( numRecords ) ;

		return eventRecords ;

	}

	private String[] getExcludedKeys ( String searchString ) {

		if ( searchString.contains( "isDataRequired=false" ) ) {

			return new String[] {
					"data"
			} ;

		} else {

			return new String[] {} ;

		}

	}

	private String getSortString ( String searchString ) {

		if ( searchString.contains( "eventReceivedOn=false" ) ) {

			return "createdOn.mongoDate" ;

		} else {

			return "createdOn.lastUpdatedOn" ;

		}

	}

	/**
	 *
	 * get active projects
	 *
	 * @param numberOfDays
	 * @param category
	 * @return
	 */
	@Cacheable ( value = "oneHourCache" , key = "{'getAppIdProject' + #numberOfDays + #category}" )
	public List<Document> getAppIdProject ( int numberOfDays , String category ) {

		var timer = metricUtilities.startTimer( ) ;
		Document query = new Document( ) ;

		if ( StringUtils.isNotBlank( category ) ) {

			query.append( "category", category ) ;

		}

		if ( numberOfDays > 0 ) {

			Calendar calendar = Calendar.getInstance( TimeZone.getTimeZone( "CST" ) ) ;
			calendar.add( Calendar.DAY_OF_YEAR, -( numberOfDays ) ) ;
			query.append( "createdOn.lastUpdatedOn", new Document( "$gte", calendar.getTime( ) ) ) ;

		}

		Document match = new Document( "$match", query ) ;

		Document groupFields = new Document( "_id", "$appId" ) ;
		groupFields.put( "projects", new Document( "$addToSet", "$project" ) ) ;
		Document group = new Document( "$group", groupFields ) ;

		Document projectFields = new Document( "appId", "$_id" ) ;
		projectFields.put( "_id", 0 ) ;
		projectFields.put( "projects", 1 ) ;
		Document project = new Document( "$project", projectFields ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( group ) ;
		operations.add( project ) ;
		AggregateIterable<Document> aggregationOutput = eventDataHelper.getMongoEventCollection( )
				.aggregate( operations ) ;

		var nanos = metricUtilities.stopTimer( timer, "getAppIdProject" ) ;
		logger.debug( "Time Taken {}, lifecycles: {}, fields: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				JSON.serialize( aggregationOutput ),
				query ) ;

		List<Document> result = new ArrayList<Document>( ) ;
		aggregationOutput.iterator( ).forEachRemaining( aggRow -> {

			result.add( aggRow ) ;

		} ) ;

		return result ;

	}

	public Document getLifecycles ( String appId , int numberOfDays , String category ) {

		var timer = metricUtilities.startTimer( ) ;

		EventMetaData metaData = new EventMetaData( ) ;
		List<Bson> appId_date_filters = new ArrayList<>( ) ;

		if ( StringUtils.isNotBlank( appId ) ) {

			appId_date_filters.add( eq( EventJsonConstants.APPID, appId ) ) ;

		}

		if ( numberOfDays > 0 ) {

			Calendar calendar = Calendar.getInstance( TimeZone.getTimeZone( "CST" ) ) ;
			calendar.add( Calendar.DAY_OF_YEAR, -( numberOfDays ) ) ;
			appId_date_filters.add(
					gte(
							EventJsonConstants.CREATED_ON_DATE,
							DateUtil.convertJavaDateToMongoCreatedDate( calendar.getTime( ) ) ) ) ;

		}

		if ( StringUtils.isNotBlank( category ) ) {

			appId_date_filters.add( eq( EventJsonConstants.CATEGORY, category ) ) ;

		}

		Bson lifecycle_filter = new Document( ) ;

		if ( appId_date_filters.size( ) > 0 ) {

			lifecycle_filter = and( appId_date_filters ) ;

		}

		metaData.setLifecycles(
				eventDataHelper.getMongoEventCollection( )
						.distinct( EventJsonConstants.LIFE, lifecycle_filter, String.class )
						.filter( lifecycle_filter )
						.maxTime( EventJsonConstants.MAX_QUERY_TIME_SECONDS, TimeUnit.SECONDS ) ) ;

		if ( metaData.getLifecycles( ).size( ) == 0 ) {

			metaData.setLifecycles( Arrays.asList( "usingDefaultList", "dev", "stage", "lt", "prod" ) ) ;

		}

		Document results = new Document( ) ;
		results.put( "lifecycles", metaData.getLifecycles( ) ) ;

		var nanos = metricUtilities.stopTimer( timer, "getLifecycles" ) ;
		logger.debug( "Time Taken {}, lifecycles: {}, filter: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				JSON.serialize( results ),
				lifecycle_filter ) ;

		return results ;

	}

	@Cacheable ( value = "oneHourCache" , key = "{'getLatestCached' + #appId + #project + #category + #life + #keepMostRecent }" )
	public Document getLatestCachedEvent (
											String category ,
											String appId ,
											String project ,
											String life ,
											int keepMostRecent ) {

		return getLatestEvent( category, appId, project, life, null, keepMostRecent ) ;

	}

	public Document getLatestEvent (
										String category ,
										String appId ,
										String project ,
										String life ,
										String host ,
										int keepMostRecent ) {

		var timer = metricUtilities.startTimer( ) ;
		Bson filter = eventDataHelper.constructBsonQuery( appId, life, project, category, host ) ;

		Document latestEvent = eventDataHelper.getMongoEventCollection( )
				.find( filter )
				.sort( descending( "createdOn.lastUpdatedOn" ) )
				.projection( excludeId( ) )
				.limit( 1 )
				.first( ) ;

		// only if needing to delete - for CSAP User settings for customized
		// views.
		eventDataHelper.keepMostRecentAndDeleteOthers( category, appId, life, keepMostRecent ) ;
		var nanos = metricUtilities.stopTimer( timer, "latestEvent" ) ;
		logger.debug( "Time Taken {}, filter: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				filter ) ;
		return latestEvent ;

	}

	public Document getDataForObjectId ( String objectId ) {

		Document eventDoc = eventDataHelper.getMongoEventCollection( )
				.find( eq( "_id", new ObjectId( objectId ) ) )
				.projection( fields( include( "data", "category", "dataKey" ), excludeId( ) ) )
				.first( ) ;

		if ( null == eventDoc ) {

			return new Document( ) ;

		}

		String category = eventDoc.getString( "category" ) ;
		MongoCollection<Document> metricsCollection = eventDataHelper.getMetricsCollectionByCategory( category ) ;

		if ( null != metricsCollection ) {

			Document metricsDocument = metricsCollection.find( eq( "_id", new ObjectId( eventDoc.getString(
					"dataKey" ) ) ) )
					.projection( excludeId( ) )
					.first( ) ;
			return metricsDocument ;

		} else {

			// Non metrics collection we don't need these
			eventDoc.remove( "category" ) ;
			eventDoc.remove( "dataKey" ) ;
			return eventDoc ;

		}

	}

	public Document getEventByObjectId ( String objectId ) {

		Document eventDoc = eventDataHelper.getMongoEventCollection( )
				.find( eq( "_id", new ObjectId( objectId ) ) )
				// .projection(fields(include("data","category","dataKey"),excludeId()))
				.first( ) ;

		if ( null == eventDoc ) {

			return new Document( ) ;

		}

		String category = eventDoc.getString( "category" ) ;
		MongoCollection<Document> metricsCollection = eventDataHelper.getMetricsCollectionByCategory( category ) ;

		if ( null != metricsCollection ) {

			Document metricsDocument = metricsCollection.find( eq( "_id", new ObjectId( eventDoc.getString(
					"dataKey" ) ) ) )
					.projection( excludeId( ) )
					.first( ) ;
			return metricsDocument ;

		} else {

			return eventDoc ;

		}

	}

	public final static String SEARCH_FILTER = "event.search-filters" ;
	public final static String SEARCH_FILTER_KEY = SEARCH_FILTER + "." ;

	/**
	 * This can be changed to java driver 3 only after 3.1 library is out
	 *
	 * @param appId
	 * @param life
	 * @return
	 */
	// @Cacheable(value = "oneMinuteCache", key = "{'metaData' + #appId + #life
	// + #appId + #fromDate + #toDate}")
	public EventMetaData getEventMetaData (
											String appId ,
											String life ,
											String fromDate ,
											String toDate ,
											int maxSeconds ) {

		var totalRefreshTime = metricUtilities.startTimer( ) ;

		EventMetaData searchFilters = new EventMetaData( ) ;

		var appidTime = metricUtilities.startTimer( ) ;

		int MAX_META_QUERY_SECONDS = maxSeconds ;
		// show all appids - enable users to switch
		searchFilters.setAppIds( eventDataHelper
				.getMongoEventCollection( )
				.distinct( APPID, String.class )
				.maxTime( MAX_META_QUERY_SECONDS, TimeUnit.SECONDS ) ) ;

		metricUtilities.stopTimer( appidTime, SEARCH_FILTER_KEY + "appids" ) ;

		List<Bson> all_filters = new ArrayList<>( ) ;
		List<Bson> appId_date_filters = new ArrayList<>( ) ;

		if ( StringUtils.isNotBlank( appId ) ) {

			all_filters.add( eq( EventJsonConstants.APPID, appId ) ) ;
			appId_date_filters.add( eq( EventJsonConstants.APPID, appId ) ) ;

		}

		if ( StringUtils.isNotBlank( fromDate ) ) {

			all_filters.add( gte( EventJsonConstants.CREATED_ON_DATE, DateUtil.convertUserDateToMongoCreatedDate(
					fromDate ) ) ) ;
			appId_date_filters.add( gte( EventJsonConstants.CREATED_ON_DATE, DateUtil.convertUserDateToMongoCreatedDate(
					fromDate ) ) ) ;

		}

		if ( StringUtils.isNotBlank( toDate ) ) {

			all_filters.add( lte( EventJsonConstants.CREATED_ON_DATE, DateUtil.convertUserDateToMongoCreatedDate(
					toDate ) ) ) ;
			appId_date_filters.add( lte( EventJsonConstants.CREATED_ON_DATE, DateUtil.convertUserDateToMongoCreatedDate(
					toDate ) ) ) ;

		}

		Bson lifecycle_filter = new Document( ) ;

		if ( appId_date_filters.size( ) > 0 ) {

			lifecycle_filter = and( appId_date_filters ) ;

		}

		var lifeTimer = metricUtilities.startTimer( ) ;
		searchFilters.setLifecycles(
				eventDataHelper.getMongoEventCollection( )
						.distinct( EventJsonConstants.LIFE, lifecycle_filter, String.class )
						.filter( lifecycle_filter )
						.maxTime( MAX_META_QUERY_SECONDS, TimeUnit.SECONDS ) ) ;
		metricUtilities.stopTimer( lifeTimer, SEARCH_FILTER_KEY + "lifecycles" ) ;

		if ( StringUtils.isNotBlank( life ) ) {

			all_filters.add( eq( EventJsonConstants.LIFE, life ) ) ;

		}

		Bson allFiltersAnded = new Document( ) ;

		if ( all_filters.size( ) > 0 ) {

			allFiltersAnded = and( all_filters ) ;

		}

		addCategorysHostsAndProjects( searchFilters, MAX_META_QUERY_SECONDS, allFiltersAnded ) ;

		addActiveUsers( searchFilters, MAX_META_QUERY_SECONDS, all_filters ) ;

		var nanos = metricUtilities.stopTimer( totalRefreshTime, "csap." + SEARCH_FILTER ) ;

		logger.debug( "all - filter: {}, Time taken: {},\n length: {}",
				allFiltersAnded,
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				searchFilters.toString( ).length( ) ) ;

		// logger.debug( "Time taken: {}\n {}", SimonUtils.presentNanoTime(
		// totalRefreshTime.runningFor() ), metaData.toString() );
		return searchFilters ;

	}

	private void addCategorysHostsAndProjects (
												EventMetaData metaData ,
												int MAX_META_QUERY_SECONDS ,
												Bson allFiltersAnded ) {

		var timer = metricUtilities.startTimer( ) ;

		metaData.setCategories(
				eventDataHelper.getMongoEventCollection( )
						.distinct( EventJsonConstants.CATEGORY, String.class )
						.filter( allFiltersAnded )
						.maxTime( MAX_META_QUERY_SECONDS, TimeUnit.SECONDS ) ) ;

		metricUtilities.stopTimer( timer, SEARCH_FILTER_KEY + "category" ) ;

		//
		var hosts = metricUtilities.startTimer( ) ;

		metaData.setHosts(
				eventDataHelper.getMongoEventCollection( )
						.distinct(
								EventJsonConstants.HOST,
								String.class )
						.filter( allFiltersAnded )
						.maxTime( MAX_META_QUERY_SECONDS, TimeUnit.SECONDS ) ) ;
		metricUtilities.stopTimer( hosts, SEARCH_FILTER_KEY + "hosts" ) ;

		logger.debug( "{} hosts: {} ", allFiltersAnded, metaData.getHosts( ) ) ;

		var projects = metricUtilities.startTimer( ) ;
		metaData.setProjects(
				eventDataHelper.getMongoEventCollection( )
						.distinct( EventJsonConstants.PROJECT, String.class )
						.filter( allFiltersAnded )
						.maxTime( MAX_META_QUERY_SECONDS, TimeUnit.SECONDS ) ) ;
		metricUtilities.stopTimer( projects, SEARCH_FILTER_KEY + "projects" ) ;

	}

	private void addActiveUsers ( EventMetaData metaData , int MAX_META_QUERY_SECONDS , List<Bson> all_filters ) {

		var timer = metricUtilities.startTimer( ) ;

		List<Bson> user_filters = new ArrayList<>( ) ;

		// ignore null users - seems to be failing intermittently
		// user_filters.add( ne( EventJsonConstants.UI_USER, null ) ) ;
		Pattern uiPattern = Pattern.compile( "^/csap/ui/" ) ;
		user_filters.add( regex( EventJsonConstants.CATEGORY, uiPattern ) ) ;
		// switch to use of category

		if ( all_filters.size( ) > 0 ) {

			user_filters.addAll( all_filters ) ;

		}

		Bson usersDistinctFilter = and( user_filters ) ;
		metaData.setUiUsers(
				eventDataHelper.getMongoEventCollection( )
						.distinct( EventJsonConstants.UI_USER, String.class )
						.filter( usersDistinctFilter )
						.maxTime( MAX_META_QUERY_SECONDS, TimeUnit.SECONDS ) ) ;

		var nanos = metricUtilities.stopTimer( timer, SEARCH_FILTER_KEY + "users" ) ;

		logger.debug( "users - filter: {}, Time taken: {},\n users: {}",
				usersDistinctFilter,
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				metaData.getUiUsers( ) ) ;

	}

	@Inject
	ObjectMapper jsonMapper ;

	public JsonNode retrieveLogRotateReport (
												String appId ,
												String project ,
												String life ,
												String category ,
												String fromDate ,
												String toDate ) {

		List<Document> operations = new ArrayList<>( ) ;

		Document query = eventDataHelper.constructDocumentQuery( appId, life, project, category ) ;
		eventDataHelper.addDateToQuery( query, fromDate, toDate ) ;
		Document match = new Document( "$match", query ) ;
		operations.add( match ) ;

		logger.info( " filter: {}", query ) ;

		Document unwind = new Document( "$unwind", "$data.summary" ) ;
		operations.add( unwind ) ;

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( "serviceName", "$data.summary.serviceName" ) ;
		groupFieldMap.put( "appId", "$appId" ) ;
		groupFieldMap.put( "project", "$project" ) ;
		groupFieldMap.put( "lifecycle", "$lifecycle" ) ;
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

		groupFields.append( "Count", new BasicDBObject( "$sum", "$data.summary.Count" ) ) ;
		groupFields.append( "MeanSeconds", new BasicDBObject( "$sum", "$data.summary.MeanSeconds" ) ) ;
		groupFields.append( "TotalSeconds", new BasicDBObject( "$sum", "$data.summary.TotalSeconds" ) ) ;
		Document group = new Document( "$group", groupFields ) ;
		operations.add( group ) ;

		AggregateIterable<Document> aggregationOutput = eventDataHelper.getMongoEventCollection( )
				.aggregate( operations ) ;

		ArrayNode logSummaries = jsonMapper.createArrayNode( ) ;
		aggregationOutput.iterator( ).forEachRemaining( doc -> {

			logger.debug( "aggregationOutput: {}", doc.toJson( ) ) ;

			try {

				logSummaries.add( jsonMapper.readTree( doc.toJson( ) ) ) ;

			} catch ( Exception e ) {

				logger.warn( "Failed to unmarshal: {}", CSAP.buildCsapStack( e ) ) ;

			}

		} ) ;

		return logSummaries ;

	}

	private BasicDBObject constructQuery ( String appId , String life ) {

		BasicDBObject query = new BasicDBObject( ) ;

		if ( StringUtils.isNoneBlank( appId ) ) {

			query.append( APPID, appId ) ;

		}

		if ( StringUtils.isNoneBlank( life ) ) {

			query.append( LIFE, life ) ;

		}

		return query ;

	}

	public BasicDBObject constructQuery ( String appId , String project , String life , String category ) {

		BasicDBObject query = new BasicDBObject( ) ;

		if ( StringUtils.isNotBlank( appId ) ) {

			query.append( "appId", appId ) ;

		}

		if ( StringUtils.isNotBlank( project ) ) {

			query.append( "project", project ) ;

		}

		if ( StringUtils.isNotBlank( life ) ) {

			query.append( "lifecycle", life ) ;

		}

		if ( StringUtils.isNotBlank( category ) ) {

			query.append( "category", category ) ;

		}

		return query ;

	}

	@Cacheable ( value = "oneMinuteCache" , key = "{'numberOfEvents' + #searchString}" )
	public Long numberOfEvents ( String searchString ) {

		logger.debug( "Event count not found in cache or expired, updating using: {}", searchString ) ;

		var timer = metricUtilities.startTimer( ) ;
		MongoCollection<Document> eventCollection = eventDataHelper.getMongoEventCollection( ) ;

		Bson filter = eventDataHelper.convertUserInterfaceQueryToMongoFilter( searchString ) ;

		logger.debug( "search filter: {}", filter ) ;
		CountOptions countOptions = new CountOptions( ) ;
		countOptions.maxTime( EventJsonConstants.MAX_QUERY_TIME_SECONDS, TimeUnit.SECONDS ) ;

		if ( searchString.contains( "appId" ) ) {

			countOptions.hintString( EventJsonConstants.EVENT_ALL_INDEX ) ;

		} else if ( searchString.contains( "from" ) ) {

			countOptions.hintString( EventJsonConstants.EVENT_DATE_INDEX ) ;

		}

		long count = -1 ;

		try {

			count = eventCollection.count( filter, countOptions ) ;

		} catch ( Exception e ) {

			logger.error( "Failed to find {} in 20 seconds", searchString ) ;

		}

		metricUtilities.stopTimer( timer, "filteredTotalRecords" ) ;
		return new Long( count ) ;

	}

	@Cacheable ( value = "oneMinuteCache" , key = "{'totalEvents' + #userRequest}" )
	public long getTotalEvents ( String userRequest ) {

		var timer = metricUtilities.startTimer( ) ;
		CountOptions countOptions = new CountOptions( ) ;
		countOptions.maxTime( EventJsonConstants.MAX_QUERY_TIME_SECONDS, TimeUnit.SECONDS ) ;
		// countOptions.hintString( "appId_1_lifecycle_1_createdOn.date_-1" );
		long count = 99999 ;
		Bson filter = eventDataHelper.buildAppIdAndLifeFilter( userRequest ) ;

		try {

			count = eventDataHelper.getMongoEventCollection( )
					.count(
							filter,
							countOptions ) ;

		} catch ( Exception e ) {

			logger.error( "Failed to find {} in 20 seconds", filter ) ;

		}

		metricUtilities.stopTimer( timer, "totalRecords" ) ;
		return count ;

	}

	@Cacheable ( value = "oneHourCache" , key = "{'dataSize'}" )
	public double getDataSize ( ) {

		Document dbStatsCommand = new Document( ) ;
		dbStatsCommand.put( "dbStats", 1 ) ;
		// dbStatsCommand.put("scale", 1024);
		Document dbStats = mongoClient.getDatabase( "event" ).runCommand( dbStatsCommand, ReadPreference
				.secondary( ) ) ;
		logger.debug( "dbStats {}", dbStats ) ;
		double dataSize = dbStats.getDouble( "dataSize" ) ;
		return dataSize ;

	}

	public long countEvents ( int numDays , String appId , String life , String category , String project ) {

		List<Bson> conds = new ArrayList<>( ) ;

		if ( StringUtils.isNotBlank( appId ) ) {

			conds.add( eq( "appId", appId ) ) ;

		}

		if ( StringUtils.isNotBlank( project ) ) {

			conds.add( eq( "project", project ) ) ;

		}

		if ( StringUtils.isNotBlank( life ) ) {

			conds.add( eq( "lifecycle", life ) ) ;

		}

		if ( StringUtils.isNotBlank( category ) ) {

			boolean useRegEx = false ;
			useRegEx = eventDataHelper.isUseRegEx( category ) ;

			if ( useRegEx ) {

				String categorySubString = category ;

				if ( category.endsWith( "*" ) ) {

					categorySubString = category.substring( 0, category.indexOf( "*" ) ) ;

				}

				Pattern pattern = Pattern.compile( "^" + categorySubString, Pattern.CASE_INSENSITIVE ) ;
				conds.add( regex( "category", pattern ) ) ;

			} else {

				conds.add( eq( "category", category ) ) ;

			}

		}

		if ( numDays > 0 ) {

			Calendar calendar = Calendar.getInstance( ) ;
			calendar.add( Calendar.DAY_OF_YEAR, ( -numDays ) ) ;
			conds.add( gte( "createdOn.lastUpdatedOn", calendar.getTime( ) ) ) ;

		}

		Bson filter = new Document( ) ;

		if ( conds.size( ) > 0 ) {

			filter = and( conds ) ;

		}

		MongoCollection<Document> eventCollection = eventDataHelper.getMongoEventCollection( ) ;
		long count = eventCollection.count( filter ) ;
		logger.debug( "Number of events for appId: {} ,project: {} , life: {} , category: {} , numDays: {} is {} ",
				appId, project, life, category, numDays, count ) ;
		return count ;

	}

	private BasicDBObject constructDateQuery ( int numDays ) {

		BasicDBObject query = new BasicDBObject( ) ;

		if ( numDays > 0 ) {

			Calendar calendar = Calendar.getInstance( TimeZone.getTimeZone( "CST" ) ) ;
			calendar.add( Calendar.DAY_OF_YEAR, -( numDays ) ) ;
			query.append( CREATED_ON + "." + LAST_UPDATED_ON, new BasicDBObject( "$gte", calendar.getTime( ) ) ) ;

		}

		return query ;

	}

	// public DBCursor getMetricsRecords(Integer numRecords, Integer startIndex,
	// String category) {
	// DBCollection collection = getCollectionForCategory( category );
	// BasicDBObject sortOrder = new BasicDBObject( "$natural", -1 );
	// DBCursor cursor = collection.find().sort( sortOrder );
	// cursor.setReadPreference( ReadPreference.secondaryPreferred() );
	// //cursor.skip(startIndex);
	// cursor.limit( -numRecords );
	// logger.debug( "Cursor{}", cursor.toString() );
	// return cursor;
	// }
	//
	// @Deprecated
	// private DBCollection getCollectionForCategory(String category) {
	// DBCollection dbCollection = null;
	// //pass the category to db and get actual db name and collection name
	// if ( category.startsWith( "/csap/metrics" ) && category.endsWith( "data"
	// ) ) {
	// dbCollection = mongoClient.getDB( "metricsDb" ).getCollection( "metrics"
	// );
	// } else if ( category.startsWith( "/csap/metrics" ) && category.endsWith(
	// "attributes" ) ) {
	// dbCollection = mongoClient.getDB( METRICS_DB_NAME ).getCollection(
	// METRICS_ATTRIBUTES_COLLECTION_NAME );
	// }
	// return dbCollection;
	// }
}
