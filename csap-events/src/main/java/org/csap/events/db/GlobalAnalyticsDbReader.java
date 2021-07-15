package org.csap.events.db ;

import static com.mongodb.client.model.Filters.and ;
import static com.mongodb.client.model.Filters.eq ;
import static com.mongodb.client.model.Filters.gte ;

import java.util.ArrayList ;
import java.util.Arrays ;
import java.util.Calendar ;
import java.util.HashMap ;
import java.util.List ;
import java.util.Map ;
import java.util.Objects ;
import java.util.TimeZone ;
import java.util.concurrent.TimeUnit ;
import java.util.stream.Collectors ;

import javax.inject.Inject ;

import org.bson.Document ;
import org.bson.conversions.Bson ;
import org.csap.events.util.DateUtil ;
import org.csap.events.util.EventJsonConstants ;
import org.csap.events.util.GraphData ;
import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapApplication ;
import org.csap.integations.CsapInformation ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;

import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.client.AggregateIterable ;
import com.mongodb.client.DistinctIterable ;
import com.mongodb.client.FindIterable ;
import com.mongodb.client.MongoCursor ;
import com.mongodb.util.JSON ;

public class GlobalAnalyticsDbReader {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Inject
	private MetricsDataReader metricsDataReader ;
	@Autowired
	CsapInformation csapInfo ;

	@Inject
	private AnalyticsHelper analyticsHelper ;

	public String getHealthErrorMessages ( String projectName ) {

		Document query = constructHealthMessageQuery( projectName ) ;
		FindIterable<Document> result = analyticsHelper.getMongoEventCollection( ).find( query ) ;
		return JSON.serialize( result ) ;

	}

	public Map<String, Map> getHealthInfo ( ) {

		Document match = new Document( "$match", constructHealthQuery( ) ) ;

		Document groupFields = new Document( "_id", "$project" ) ;

		ArrayList trueCondition = new ArrayList( ) ;
		trueCondition.add( "$data.Healthy" ) ;
		trueCondition.add( true ) ;
		ArrayList trueCondArray = new ArrayList( ) ;
		trueCondArray.add( new Document( "$eq", trueCondition ) ) ;
		trueCondArray.add( 1 ) ;
		trueCondArray.add( 0 ) ;
		Document healthyCondition = new Document( "$cond", trueCondArray ) ;

		ArrayList falseCondition = new ArrayList( ) ;
		falseCondition.add( "$data.Healthy" ) ;
		falseCondition.add( false ) ;
		ArrayList falseCondArray = new ArrayList( ) ;
		falseCondArray.add( new Document( "$eq", falseCondition ) ) ;
		falseCondArray.add( 1 ) ;
		falseCondArray.add( 0 ) ;
		Document unhealthyCondition = new Document( "$cond", falseCondArray ) ;

		groupFields.put( "healthyvm", new Document( "$sum", healthyCondition ) ) ;
		groupFields.put( "unhealthyvm", new Document( "$sum", unhealthyCondition ) ) ;
		groupFields.put( "totalvm", new Document( "$sum", 1 ) ) ;
		Document group = new Document( "$group", groupFields ) ;

		Document projectFields = new Document( "projectName", "$_id" ) ;
		projectFields.put( "healthyvm", 1 ) ;
		projectFields.put( "unhealthyvm", 1 ) ;
		projectFields.put( "totalvm", 1 ) ;
		projectFields.put( "_id", 0 ) ;
		Document project = new Document( "$project", projectFields ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( group ) ;
		// operations.add(project);

		AggregateIterable<Document> output = analyticsHelper.getMongoEventCollection( ).aggregate( operations ) ;
		Map<String, Map> healthMap = new HashMap<>( ) ;
		MongoCursor<Document> resultCursor = output.iterator( ) ;

		while ( resultCursor.hasNext( ) ) {

			Document resultDocument = resultCursor.next( ) ;
			Map health = new HashMap( ) ;
			health.put( "healthyvm", resultDocument.get( "healthyvm" ) ) ;
			health.put( "unhealthyvm", resultDocument.get( "unhealthyvm" ) ) ;
			health.put( "totalvm", resultDocument.get( "totalvm" ) ) ;
			healthMap.put( (String) resultDocument.get( "_id" ), health ) ;

		}

		logger.debug( "health map {} ", healthMap ) ;

		return healthMap ;

	}

	public Map<String, Document> getPackageSumnmarysByLife ( String packageName , String appId ) {

		List<String> lifecycles = getLifesForAppId( appId ) ;

		var timer = metricUtilities.startTimer( ) ;
		logger.debug( "Life list{}", lifecycles ) ;
		Document sortOrder = new Document( "createdOn.lastUpdatedOn", -1 ) ;
		Map<String, Document> lifeToService = new HashMap<>( ) ;

		lifeToService = lifecycles
				.stream( )
				.map( lifecycle -> findPackageSummary( lifecycle, packageName ) )
				.filter( Objects::nonNull )
				.collect(
						Collectors.toMap(
								serviceDataForLifecycle -> (String) serviceDataForLifecycle.get( "tempLife" ),
								serviceDataForLifecycle -> serviceDataForLifecycle ) ) ;

		var nanos = metricUtilities.stopTimer( timer, "InstanceCount" ) ;

		logger.debug( "Time Taken {}, instance: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				lifeToService.keySet( ) ) ;

		return lifeToService ;

	}

	private Document findPackageSummary ( String lifecycle , String packageName ) {

		Document serviceDataForLifecycle = null ;
		Document query = new Document( ) ;
		query.append( EventJsonConstants.CATEGORY, EventJsonConstants.CSAP_MODEL_SUMMAY_CATEGORY ) ;
		// query.append("project", projectName);
		Document projectMatch = new Document( "package", packageName ) ;
		query.append( "data.packages", new Document( "$elemMatch", projectMatch ) ) ;
		query.append( "lifecycle", lifecycle ) ;
		Document result = analyticsHelper.getMongoEventCollection( )
				.find( query )
				.sort( new Document( EventJsonConstants.CREATED_ON_LAST_UPDATED, -1 ) )
				.limit( 1 )
				.first( ) ;

		if ( null != result ) {

			Document dataObject = (Document) result.get( "data" ) ;
			List<Document> releasePackages = (List<Document>) dataObject.get( "packages" ) ;

			for ( Document csapPackage : releasePackages ) {

				if ( packageName.equalsIgnoreCase( (String) csapPackage.get( "package" ) ) ) {

					// if ( lifecycle.equals( "prod") ) continue;
					serviceDataForLifecycle = csapPackage ;
					serviceDataForLifecycle.put( "tempLife", lifecycle ) ;
					serviceDataForLifecycle.put( "projectUrl", dataObject.get( "projectUrl" ) ) ;
					break ;

				}

			}

		}

		return serviceDataForLifecycle ;

	}

	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	public ObjectNode reportCounts ( ) {

		var timer = metricUtilities.startTimer( ) ;

		var hostName = csapInfo.getHostShortName( ) ;

		if ( ! CsapApplication.isCsapFolderSet( ) ) {

			hostName = "csap-dev01" ;

		}

		long daysAvailableForHostData = metricsDataReader.retrieveNumDaysOfMetrics( hostName ) ;
		long hostReportCount = analyticsHelper.getEventCount( "/csap/reports/host/daily", 0, 0 ) ;
		long serviceReportCount = analyticsHelper.getEventCount( "/csap/reports/process/daily", 0, 0 ) ;
		long serviceReportCountOneDay = analyticsHelper.getEventCount( "/csap/reports/process/daily", 1, 1 ) ;
		ObjectNode dataNode = jacksonMapper.createObjectNode( ) ;
		dataNode.put( "hostReportCount", hostReportCount ) ;
		dataNode.put( "daysAvailableForHostData", daysAvailableForHostData ) ;
		dataNode.put( "serviceReportCount", serviceReportCount ) ;
		dataNode.put( "serviceReportCountOneDay", serviceReportCountOneDay ) ;
		dataNode.put( "life", System.getenv( ).get( "csapLife" ) ) ;
		var nanos = metricUtilities.stopTimer( timer, "reportCounts" ) ;

		logger.debug( "Time Taken {}, instance: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				dataNode ) ;

		return dataNode ;

	}

	public AggregateIterable<Document> getAnalyticsData ( int numDays ) {

		List<String> avgAttributes = Arrays.asList( "vms", "serviceCount", "instanceCount", "cpuCount", "totalLoad",
				"numSamples",
				"activeUsers", "totalActivity" ) ;
		List<String> firstAttributes = Arrays.asList( "csapVersion", "deploymentName", "appId" ) ;

		Document match = new Document( "$match", buildQueryForCsapGlobalReport( numDays ) ) ;

		Document unwind = new Document( "$unwind", "$data" ) ;

		Document groupFields = new Document( "_id", "$data.projectName" ) ;
		avgAttributes.forEach( attribute -> groupFields.append( attribute, new Document( "$avg", "$data."
				+ attribute ) ) ) ;
		firstAttributes.forEach( attribute -> groupFields.append( attribute, new Document( "$first", "$data."
				+ attribute ) ) ) ;
		Document group = new Document( "$group", groupFields ) ;

		Document projectFields = new Document( "projectName", "$_id" ) ;
		avgAttributes.forEach( attribute -> projectFields.put( attribute, 1 ) ) ;
		firstAttributes.forEach( attribute -> projectFields.put( attribute, 1 ) ) ;
		Document project = new Document( "$project", projectFields ) ;

		Document sortOrder = new Document( "createdOn.date", -1 ) ;
		Document sort = new Document( "$sort", sortOrder ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( sort ) ;
		operations.add( unwind ) ;
		operations.add( group ) ;
		operations.add( project ) ;

		// DBCollection eventCollection = analyticsHelper.getEventCollection();
		AggregateIterable<Document> output = analyticsHelper.getMongoEventCollection( ).aggregate( operations ) ;

		logger.debug( "Results {} ", output ) ;
		return output ;

	}

	public GraphData buildProjectAdoptionTrends ( int numDays ) {

		Document reportsQuery = buildQueryForCsapGlobalReport( numDays ) ;
		Document sortOrder = new Document( "createdOn.date", -1 ) ;

		logger.info( "Getting analytics days: {}, query: {}", numDays, reportsQuery.toString( ) ) ;

		FindIterable<Document> result = analyticsHelper
				.getMongoEventCollection( )
				.find( reportsQuery )
				.sort( sortOrder ) ;

		MongoCursor<Document> resultCursor = result.iterator( ) ;
		GraphData graphData = new GraphData( ) ;

		while ( resultCursor.hasNext( ) ) {

			Document resultDocument = resultCursor.next( ) ;
			Document createdOn = (Document) resultDocument.get( "createdOn" ) ;
			String date = (String) createdOn.get( "date" ) ;
			Calendar cal = DateUtil.getDateFromString( date ) ;
			long dateAsLong = cal.getTimeInMillis( ) ;
			List dataList = (List) resultDocument.get( "data" ) ;
			dataList.forEach( dbObject -> graphData.addGraphPoint( dbObject, dateAsLong ) ) ;

		}

		return graphData ;

	}

	private Document constructHealthMessageQuery ( String projectName ) {

		String formatedDate = DateUtil.buildMongoCreatedDateFromOffset( 0 ) ;
		Document query = new Document( ) ;
		query.append( EventJsonConstants.CATEGORY, "/csap/health" ) ;
		query.append( "createdOn.date", formatedDate ) ;
		query.append( "project", projectName ) ;
		query.append( "data.Healthy", false ) ;
		return query ;

	}

	private Document constructHealthQuery ( ) {

		String formatedDate = DateUtil.buildMongoCreatedDateFromOffset( 0 ) ;
		Document query = new Document( ) ;
		query.append( EventJsonConstants.CATEGORY, "/csap/health" ) ;
		query.append( "createdOn.date", formatedDate ) ;
		return query ;

	}

	public static final String CSAP_DAILY_HOST_CATEGORY = "/csap/reports/host/daily" ;

	private List<String> getLifesForAppId ( String appId ) {

		var timer = metricUtilities.startTimer( ) ;

		Calendar calendar = Calendar.getInstance( TimeZone.getTimeZone( "CST" ) ) ;
		calendar.add( Calendar.DAY_OF_YEAR, -( 14 ) ) ;
		Bson filter = and(
				eq( EventJsonConstants.APPID, appId ),
				eq( EventJsonConstants.CATEGORY, CSAP_DAILY_HOST_CATEGORY ),
				gte(
						EventJsonConstants.CREATED_ON_DATE,
						DateUtil.convertJavaDateToMongoCreatedDate( calendar.getTime( ) ) ) ) ;

		List<String> lifes = new ArrayList<>( ) ;

		try {

			DistinctIterable<String> lifecyclesIterable = analyticsHelper.getMongoEventCollection( )
					.distinct( EventJsonConstants.LIFE, String.class )
					.filter( filter )
					.maxTime( EventJsonConstants.MAX_QUERY_TIME_SECONDS, TimeUnit.SECONDS ) ;

			lifecyclesIterable.iterator( ).forEachRemaining( lifes::add ) ;

		} catch ( Exception e ) {

			lifes.add( "not able to find" ) ;
			logger.warn( "Failed to get response", e ) ;

		}

		var nanos = metricUtilities.stopTimer( timer, "getLifecycles" ) ;
		logger.debug( "Time Taken {}, lifecycles: {}, filter: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				lifes,
				filter ) ;

		return lifes ;

	}

	private Document buildQueryForCsapGlobalReport ( int numDays ) {

		String formatedDate = DateUtil.buildMongoCreatedDateFromOffset( numDays ) ;
		Document query = new Document( ) ;
		query.append( EventJsonConstants.CATEGORY, EventJsonConstants.CSAP_GLOBAL_REPORT_CATEGORY ) ;

		if ( numDays == 1 ) {

			query.append( "createdOn.date", formatedDate ) ;

		} else if ( numDays > 1 ) {

			query.append( "createdOn.date", new Document( "$gte", formatedDate ) ) ;

		}

		return query ;

	}

}
