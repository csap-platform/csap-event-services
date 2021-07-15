package org.csap.events.db ;

import static org.csap.events.util.MetricsJsonConstants.$PROJECT ;
import static org.csap.events.util.MetricsJsonConstants.CATEGORY ;
import static org.csap.events.util.MetricsJsonConstants.CREATED_ON ;
import static org.csap.events.util.MetricsJsonConstants.METADATA ;
import static org.csap.events.util.MetricsJsonConstants.MONGO_DATE ;
import static org.csap.events.util.MetricsJsonConstants.PROJECT ;
import static org.csap.events.util.MetricsJsonConstants.UIUSER ;
import static org.csap.events.util.MongoConstants.EVENT_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.EVENT_DB_NAME ;

import java.time.LocalDateTime ;
import java.time.format.DateTimeFormatter ;
import java.util.ArrayList ;
import java.util.Calendar ;
import java.util.HashMap ;
import java.util.List ;
import java.util.Map ;
import java.util.regex.Pattern ;
import java.util.stream.StreamSupport ;

import javax.inject.Inject ;

import org.apache.commons.lang3.StringUtils ;
import org.bson.Document ;
import org.csap.events.EventServiceConfiguration ;
import org.csap.events.util.DateUtil ;
import org.csap.events.util.EventJsonConstants ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.stereotype.Service ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ArrayNode ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.BasicDBObject ;
import com.mongodb.DBCollection ;
import com.mongodb.MongoClient ;
import com.mongodb.client.AggregateIterable ;
import com.mongodb.client.MongoCollection ;
import com.mongodb.client.MongoCursor ;
import com.mongodb.util.JSON ;

@Service
public class CsapAdoptionReportBuilder {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Inject
	private MongoClient mongoClient ;

	@Inject
	private AnalyticsHelper analyticsHelper ;

	@Inject
	private EventServiceConfiguration eventHelper ;

	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	public String buildAdoptionReportAndSaveToDB ( int offSet , String projectName ) {

		Map<String, GlobalAnalyticsSummary> allGlobalActivity = new HashMap<>( ) ;
		List lifes = getLife( offSet, projectName ) ;
		retrieveInfoFromClusterConfig( offSet, allGlobalActivity, projectName, lifes ) ;

		if ( allGlobalActivity.size( ) > 0 ) {

			retrieveCpuInformation( offSet, allGlobalActivity ) ;
			retrieveLoadInformation( offSet, allGlobalActivity ) ;
			activeUsers( offSet, allGlobalActivity ) ;
			userActivity( offSet, allGlobalActivity ) ;
			retrieveCurrentGlobalActivity( allGlobalActivity, projectName, offSet ) ;
			calculateTotal( allGlobalActivity ) ;
			String jsonDoc = summaryJson( allGlobalActivity, offSet ) ;
			eventHelper.postEventData( jsonDoc ) ;
			logger.debug( "{} offSet: {} days,  report: \n{}", projectName, offSet, jsonDoc ) ;
			return jsonDoc ;

		}

		return "No data to post" ;

	}

	public String buildAdoptionReportAndSaveToDB ( int offSet ) {

		String result = "Did not run" ;

		Map<String, GlobalAnalyticsSummary> allGlobalActivity = new HashMap<>( ) ;

		AggregateIterable<Document> projectWithLife = getProjectLife( offSet ) ;

		retrieveInfoFromApplicationDefinition( offSet, allGlobalActivity, projectWithLife ) ;

		if ( allGlobalActivity.size( ) > 0 ) {

			retrieveCpuInformation( offSet, allGlobalActivity ) ;
			retrieveLoadInformation( offSet, allGlobalActivity ) ;
			activeUsers( offSet, allGlobalActivity ) ;
			userActivity( offSet, allGlobalActivity ) ;
			calculateTotal( allGlobalActivity ) ;
			String jsonDoc = summaryJson( allGlobalActivity, offSet ) ;
			result = eventHelper.postEventData( jsonDoc ) ;
			logger.debug( "offSet: {} days,  report: \n{}", offSet, jsonDoc ) ;

		}

		return result ;

	}

	private String summaryJson ( Map<String, GlobalAnalyticsSummary> allGlobalActivity , int offSet ) {

		ObjectNode rootNode = jacksonMapper.createObjectNode( ) ;
		rootNode.put( CATEGORY, EventJsonConstants.CSAP_GLOBAL_REPORT_CATEGORY ) ;
		ObjectNode createdOn = jacksonMapper.createObjectNode( ) ;
		Calendar calendar = Calendar.getInstance( ) ;
		calendar.add( Calendar.DAY_OF_YEAR, -( offSet ) ) ;
		createdOn.put( "unixMs", calendar.getTimeInMillis( ) ) ;
		createdOn.put( "date", DateUtil.convertJavaCalendarToMongoCreatedDate( calendar ) ) ;
		createdOn.put( "time", DateUtil.getFormatedTime( calendar ) ) ;

		String now = LocalDateTime.now( ).format( DateTimeFormatter.ofPattern( "MMM.d.yyyy-HH.mm.ss" ) ) ;
		createdOn.put( "reportRun", now ) ;

		rootNode.set( "createdOn", createdOn ) ;
		rootNode.put( "summary", "Global Analytics Summary" ) ;
		String life = System.getenv( "csapLife" ) ;

		// logger.debug("life::{}",life);
		if ( StringUtils.isBlank( life ) ) {

			life = "prod" ;

		}

		rootNode.put( "lifecycle", life ) ;
		rootNode.put( PROJECT, "CsapData" ) ;
		rootNode.put( "appId", "csapeng.gen" ) ;
		rootNode.put( "host", analyticsHelper.getHostName( ) ) ;
		ArrayNode arrayNode = jacksonMapper.createArrayNode( ) ;
		allGlobalActivity.values( ).forEach( summary -> {

			try {

				JsonNode node = jacksonMapper.readTree( jacksonMapper.writeValueAsString( summary ) ) ;
				arrayNode.add( node ) ;

			} catch ( Exception e ) {

				logger.error( "Exception while converting analytics", e ) ;

			}

		} ) ;
		rootNode.set( "data", arrayNode ) ;
		String jsonDoc = "" ;

		try {

			jsonDoc = jacksonMapper.writeValueAsString( rootNode ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while converting to jsondoc" ) ;

		}

		return jsonDoc ;

	}

	public void retrieveLoadInformation ( int offSet , Map<String, GlobalAnalyticsSummary> allGlobalActivity ) {

		var timer = metricUtilities.startTimer( ) ;
		metricUtilities.stopTimer( timer, "AdoptionReport.retrieveLoadInformation" ) ;
		String formatedDate = DateUtil.buildMongoCreatedDateFromOffset( offSet ) ;
		MongoCollection<Document> eventCollection = analyticsHelper.getMongoEventCollection( ) ;

		for ( String projectName : allGlobalActivity.keySet( ) ) {

			Document query = new Document( ) ;
			query.append( CATEGORY, "/csap/reports/host/daily" ) ;
			query.append( PROJECT, projectName ) ;
			query.append( CREATED_ON + ".date", formatedDate ) ;

			Document match = new Document( "$match", query ) ;

			Document groupFields = new Document( "_id", "$" + PROJECT ) ;
			groupFields.put( "totalLoad", new Document( "$sum", "$data.summary.totalLoad" ) ) ;
			groupFields.put( "numberOfSamples", new Document( "$sum", "$data.summary.numberOfSamples" ) ) ;

			Document group = new Document( "$group", groupFields ) ;

			List<Document> operations = new ArrayList<>( ) ;
			operations.add( match ) ;
			operations.add( group ) ;

			// AggregationOutput output =
			// dbCollection.aggregate(operations,ReadPreference.secondaryPreferred());
			AggregateIterable<Document> aggregationOutput = eventCollection.aggregate( operations ) ;
			/*
			 * output.results().forEach(dbObject -> {
			 * allGlobalActivity.get(projectName).setTotalLoad(dbObject.get( "totalLoad"));
			 * allGlobalActivity.get(projectName).setNumSamples(dbObject.get(
			 * "numberOfSamples")); });
			 */
			MongoCursor<Document> resultCursor = aggregationOutput.iterator( ) ;

			// for(DBObject dbObject : output.results()){
			while ( resultCursor.hasNext( ) ) {

				Document dbObject = resultCursor.next( ) ;
				allGlobalActivity.get( projectName ).setTotalLoad( dbObject.get( "totalLoad" ) ) ;
				allGlobalActivity.get( projectName ).setNumSamples( dbObject.get( "numberOfSamples" ) ) ;

			}

		}

		metricUtilities.stopTimer( timer, "AdoptionReport.retrieveLoadInformation" ) ;

	}

	public void retrieveCpuInformation ( int offSet , Map<String, GlobalAnalyticsSummary> allGlobalActivity ) {

		var timer = metricUtilities.startTimer( ) ;

		for ( String projectName : allGlobalActivity.keySet( ) ) {

			Document query = new Document( ) ;
			query.append( CATEGORY, "/csap/health" ) ;
			query.append( PROJECT, projectName ) ;
			String formatedDate = DateUtil.buildMongoCreatedDateFromOffset( offSet ) ;
			query.append( CREATED_ON + ".date", formatedDate ) ;

			Document match = new Document( "$match", query ) ;

			Document groupFields = new Document( "_id", "$" + PROJECT ) ;
			groupFields.put( "totCpu", new Document( "$sum", "$data.vm.cpuCount" ) ) ;
			Document group = new Document( "$group", groupFields ) ;

			List<Document> operations = new ArrayList<>( ) ;
			operations.add( match ) ;
			operations.add( group ) ;

			// AggregationOutput output =
			// dbCollection.aggregate(operations,ReadPreference.secondaryPreferred());
			AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( ).aggregate(
					operations ) ;
			MongoCursor<Document> resultsCursor = aggregationOutput.iterator( ) ;

			// for(DBObject dbObject : aggregationOutput.){
			while ( resultsCursor.hasNext( ) ) {

				Document dbObject = resultsCursor.next( ) ;
				allGlobalActivity.get( projectName ).setCpuCount( dbObject.get( "totCpu" ) ) ;

			}

		}

		metricUtilities.stopTimer( timer, "AdoptionReport.retrieveCpuInformation" ) ;

	}

	public void retrieveInfoFromApplicationDefinition (
														int offSet ,
														Map<String, GlobalAnalyticsSummary> allGlobalActivity ,
														AggregateIterable<Document> projectWithLife ) {

		MongoCursor<Document> projectCursor = projectWithLife.iterator( ) ;
		// for(DBObject dbObject : projectWithLife){

		StringBuilder summaryInfo = new StringBuilder( "Generating: "
				+ EventJsonConstants.CSAP_MODEL_SUMMAY_CATEGORY ) ;

		while ( projectCursor.hasNext( ) ) {

			Document dbObject = projectCursor.next( ) ;
			String projectName = (String) dbObject.get( "_id" ) ;
			GlobalAnalyticsSummary summary = new GlobalAnalyticsSummary( ) ;
			summary.setProjectName( projectName ) ;
			List lifes = (List) dbObject.get( "lifes" ) ;

			summaryInfo.append( "\n\t Project: " + pad( projectName ) + " lifes: " + pad( lifes.toString( ) ) ) ;

			boolean dataExists = false ;

			for ( Object life : lifes ) {

				Document query = analyticsHelper.constructModelSummaryQuery( projectName, life ) ;

				Document sortOrder = new Document( CREATED_ON + "." + MONGO_DATE, -1 ) ;

				Document clusterSummary = analyticsHelper.getMongoEventCollection( )
						.find( query )
						.sort( sortOrder )
						.limit( 1 )
						.first( ) ;

				if ( null != clusterSummary ) {

					dataExists = true ;

					Document dataObject = (Document) clusterSummary.get( "data" ) ;
					summary.setCsapVersion( (String) dataObject.get( "version" ), (String) life ) ;
					summary.setDeploymentName( (String) dataObject.get( "name" ) ) ;
					summary.setAppId( (String) clusterSummary.get( "appId" ) ) ;
					List packages = (List) dataObject.get( "packages" ) ;

					packages.stream( )
							.filter( ( packObj -> isRequiredPackage( projectName, packObj ) ) )
							.findFirst( )
							.ifPresent( packObj -> summary.addVmSummary( packObj ) ) ;

				}

			}

			if ( dataExists ) {

				summaryInfo.append( " - found" ) ;
				allGlobalActivity.put( projectName, summary ) ;

			} else {

				summaryInfo.append( " - warning: missing summary" ) ;
				logger.warn(
						"Report will not include Project: {}, Lifecycles: {} - reason:  model summary not found: {}",
						projectName, lifes, EventJsonConstants.CSAP_MODEL_SUMMAY_CATEGORY ) ;

			}

		}

		logger.info( summaryInfo.toString( ) ) ;
		logger.debug( "allGlobalActivity {}", allGlobalActivity ) ;

	}

	public static String pad ( String input ) {

		return StringUtils.rightPad( input, 25 ) ;

	}

	public void retrieveInfoFromClusterConfig (
												int offSet ,
												Map<String, GlobalAnalyticsSummary> allGlobalActivity ,
												String projectName ,
												List lifes ) {

		GlobalAnalyticsSummary summary = new GlobalAnalyticsSummary( ) ;
		summary.setProjectName( projectName ) ;
		boolean dataExists = false ;

		if ( null != lifes && lifes.size( ) > 0 ) {

			for ( Object life : lifes ) {

				Document query = analyticsHelper.constructModelSummaryQuery( projectName, life ) ;
				Document sortOrder = new Document( CREATED_ON + "." + MONGO_DATE, -1 ) ;
				Document clusterSummary = analyticsHelper.getMongoEventCollection( )
						.find( query )
						.sort( sortOrder )
						.limit( 1 )
						.first( ) ;

				if ( null != clusterSummary ) {

					dataExists = true ;
					Document dataObject = (Document) clusterSummary.get( "data" ) ;
					summary.setCsapVersion( (String) dataObject.get( "version" ), (String) life ) ;
					summary.setDeploymentName( (String) dataObject.get( "name" ) ) ;
					summary.setAppId( (String) clusterSummary.get( "appId" ) ) ;

					List packages = (List) dataObject.get( "packages" ) ;

					packages.stream( )
							.filter( ( packObj -> isRequiredPackage( projectName, packObj ) ) )
							.findFirst( )
							.ifPresent( packObj -> summary.addVmSummary( packObj ) ) ;

				}

			}

		}

		if ( dataExists ) {

			allGlobalActivity.put( projectName, summary ) ;

		} else {

			logger.info( "Data does not exists. Ignored project {} ", projectName ) ;

		}

	}

	private boolean isRequiredPackage ( String projectName , Object packObj ) {

		Document packageObject = (Document) packObj ;

		if ( projectName.equalsIgnoreCase( (String) packageObject.get( "package" ) ) ) {

			return true ;

		}

		return false ;

	}

	public List getLife ( int offSet , String projectName ) {

		Document query = new Document( ) ;

		if ( offSet > 0 ) {

			Calendar startTime = DateUtil.getDateWithOffSet( offSet ) ;
			Calendar endTime = DateUtil.getDateWithOffSet( offSet - 1 ) ;
			query.append( CREATED_ON + "." + MONGO_DATE, new Document( "$gte", startTime.getTime( ) ).append( "$lt",
					endTime.getTime( ) ) ) ;

		}

		query.append( "project", projectName ) ;
		Document match = new Document( "$match", query ) ;

		Document groupFields = new Document( "_id", $PROJECT ) ;
		groupFields.put( "lifes", new Document( "$addToSet", "$lifecycle" ) ) ;
		Document group = new Document( "$group", groupFields ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( group ) ;

		// AggregationOutput lifeAggOutput =
		// eventCollection.aggregate(operations,ReadPreference.secondaryPreferred());
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( ).aggregate(
				operations ) ;
		final List lifes = new ArrayList( ) ;
		StreamSupport.stream( aggregationOutput.spliterator( ), false )
				.filter( dbObject -> projectName.equalsIgnoreCase( (String) dbObject.get( "_id" ) ) )
				.findFirst( )
				.ifPresent( lifeObj -> {

					lifes.addAll( (List) lifeObj.get( "lifes" ) ) ;

				} ) ;
		logger.debug( "Life cycles{}", lifes ) ;
		return lifes ;

	}

	public AggregateIterable<Document> getProjectLife ( int offSet ) {

		Document query = new Document( ) ;

		if ( offSet > 0 ) {

			Calendar startTime = DateUtil.getDateWithOffSet( offSet ) ;
			Calendar endTime = DateUtil.getDateWithOffSet( offSet - 1 ) ;
			query.append( CREATED_ON + "." + MONGO_DATE, new Document( "$gte", startTime.getTime( ) ).append( "$lt",
					endTime.getTime( ) ) ) ;

		}

		Document match = new Document( "$match", query ) ;

		Document groupFields = new Document( "_id", $PROJECT ) ;
		groupFields.put( "lifes", new Document( "$addToSet", "$lifecycle" ) ) ;
		Document group = new Document( "$group", groupFields ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( group ) ;

		// AggregationOutput output =
		// eventCollection.aggregate(operations,ReadPreference.secondaryPreferred());
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( ).aggregate(
				operations ) ;

		return aggregationOutput ;

	}

	private void activeUsers ( int offSet , Map<String, GlobalAnalyticsSummary> allGlobalActivity ) {

		var timer = metricUtilities.startTimer( ) ;

		Document query = new Document( ) ;
		// query.append( METADATA + "." + UIUSER, new BasicDBObject( "$exists",
		// true ) );
		Pattern uiPattern = Pattern.compile( "^/csap/ui/" ) ;
		query.append( CATEGORY, uiPattern ) ;

		Calendar startTime = DateUtil.getDateWithOffSet( offSet ) ;
		Calendar endTime = DateUtil.getDateWithOffSet( offSet - 1 ) ;
		query.append( CREATED_ON + "." + MONGO_DATE, new Document( "$gte", startTime.getTime( ) ).append( "$lt", endTime
				.getTime( ) ) ) ;
		Document match = new Document( "$match", query ) ;

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( PROJECT, "$" + PROJECT ) ;
		// groupFieldMap.put(UIUSER,"$"+METADATA+"."+UIUSER);

		Document groupFields = new Document( "_id", "$" + PROJECT ) ;
		groupFields.put( "uniqueUsers", new Document( "$addToSet", "$" + METADATA + "." + UIUSER ) ) ;
		Document group = new Document( "$group", groupFields ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( group ) ;

		// DBCollection dbCollection = getEventCollection();
		// AggregationOutput output =
		// dbCollection.aggregate(operations,ReadPreference.secondaryPreferred());
		// logger.debug("output"+output.results());
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( ).aggregate(
				operations ) ;
		MongoCursor<Document> resultsCursor = aggregationOutput.iterator( ) ;

		// for(DBObject dbObject : output.results()){
		while ( resultsCursor.hasNext( ) ) {

			Document dbObject = resultsCursor.next( ) ;
			String projectName = (String) dbObject.get( "_id" ) ;
			List uniqueUsers = (List) dbObject.get( "uniqueUsers" ) ;

			if ( null != uniqueUsers && null != allGlobalActivity.get( projectName ) ) {

				allGlobalActivity.get( projectName ).setActiveUsers( uniqueUsers.size( ) ) ;

			}

		}

		metricUtilities.stopTimer( timer, "AdoptionReport.activeUsers" ) ;

	}

	public void userActivity ( int offSet , Map<String, GlobalAnalyticsSummary> allGlobalActivity ) {

		var timer = metricUtilities.startTimer( ) ;
		Document query = new Document( ) ;
		query.append( METADATA + "." + UIUSER, new Document( "$exists", true ) ) ;
		Calendar startTime = DateUtil.getDateWithOffSet( offSet ) ;
		Calendar endTime = DateUtil.getDateWithOffSet( offSet - 1 ) ;
		query.append( CREATED_ON + "." + MONGO_DATE, new Document( "$gte", startTime.getTime( ) ).append( "$lt", endTime
				.getTime( ) ) ) ;
		Document match = new Document( "$match", query ) ;

		Document groupFields = new Document( "_id", "$" + PROJECT ) ;
		groupFields.put( "totActivity", new BasicDBObject( "$sum", 1 ) ) ;
		Document group = new Document( "$group", groupFields ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( group ) ;

		// DBCollection dbCollection = getEventCollection();
		// AggregationOutput output =
		// dbCollection.aggregate(operations,ReadPreference.secondaryPreferred());
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( ).aggregate(
				operations ) ;
		MongoCursor<Document> resultsCursor = aggregationOutput.iterator( ) ;

		while ( resultsCursor.hasNext( ) ) {

			// output.results().forEach(dbObject -> {
			Document dbObject = resultsCursor.next( ) ;
			Object projectName = dbObject.get( "_id" ) ;
			Object totActivity = dbObject.get( "totActivity" ) ;

			if ( null != allGlobalActivity.get( projectName ) ) {

				allGlobalActivity.get( projectName ).setTotalActivity( totActivity ) ;

			}

		}

		metricUtilities.stopTimer( timer, "AdoptionReport.userActivity" ) ;

	}

	private void retrieveCurrentGlobalActivity (
													Map<String, GlobalAnalyticsSummary> allGlobalActivity ,
													String projectName ,
													int offSet ) {

		String formatedDate = DateUtil.buildMongoCreatedDateFromOffset( offSet ) ;
		Document query = new Document( ) ;
		query.append( CATEGORY, EventJsonConstants.CSAP_GLOBAL_REPORT_CATEGORY ) ;
		query.append( EventJsonConstants.CREATED_ON_DATE, formatedDate ) ;
		// DBCollection eventCollection = getEventCollection();
		// DBObject dbObject = eventCollection.findOne(query);
		Document dbObject = analyticsHelper.getMongoEventCollection( )
				.find( query )
				.limit( 1 )
				.first( ) ;

		if ( null != dbObject ) {

			logger.debug( "dbobject{} ", JSON.serialize( dbObject ) ) ;
			List basicDbList = (List) dbObject.get( "data" ) ;

			if ( null != basicDbList ) {

				basicDbList.stream( )
						.filter( obj -> isRequiredGlobalActivity( projectName, obj ) )
						.forEach( obj -> {

							GlobalAnalyticsSummary projSummary = new GlobalAnalyticsSummary( (Document) obj ) ;
							allGlobalActivity.put( projSummary.getProjectName( ), projSummary ) ;

						} ) ;

			}

		}

	}

	private boolean isRequiredGlobalActivity ( String projectName , Object dbObj ) {

		Document dbObject = (Document) dbObj ;
		String currentProjectName = (String) dbObject.get( "projectName" ) ;

		if ( ! projectName.equalsIgnoreCase( currentProjectName )
				&& ! "total".equalsIgnoreCase( currentProjectName ) ) {

			return true ;

		}

		return false ;

	}

	private void calculateTotal ( Map<String, GlobalAnalyticsSummary> allGlobalActivity ) {

		GlobalAnalyticsSummary total = allGlobalActivity.values( )
				.stream( )
				.reduce( new GlobalAnalyticsSummary( "total" ), ( g1 , g2 ) -> {

					g1.add( g2 ) ;
					return g1 ;

				} ) ;
		allGlobalActivity.put( "total", total ) ;

	}

	public List getProjectNames ( ) {

		DBCollection dbCollection = getEventCollection( ) ;
		return dbCollection.distinct( "project" ) ;

	}

	private DBCollection getEventCollection ( ) {

		return mongoClient.getDB( EVENT_DB_NAME ).getCollection( EVENT_COLLECTION_NAME ) ;

	}

}
