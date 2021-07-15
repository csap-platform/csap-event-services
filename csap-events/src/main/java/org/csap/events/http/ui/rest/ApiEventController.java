package org.csap.events.http.ui.rest ;

import java.util.List ;

import javax.inject.Inject ;
import javax.servlet.http.HttpServletResponse ;

import org.bson.Document ;
import org.csap.docs.CsapDoc ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.EventJsonConstants ;
import org.csap.events.EventMetaData ;
import org.csap.events.db.EventDataHelper ;
import org.csap.events.db.EventDataReader ;
import org.csap.events.db.EventDataWriter ;
import org.csap.events.db.HealthEventWriter ;
import org.csap.integations.CsapMicroMeter ;
import org.csap.security.SpringAuthCachingFilter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.core.env.Environment ;
import org.springframework.http.HttpStatus ;
import org.springframework.http.MediaType ;
import org.springframework.web.bind.annotation.CrossOrigin ;
import org.springframework.web.bind.annotation.GetMapping ;
import org.springframework.web.bind.annotation.PathVariable ;
import org.springframework.web.bind.annotation.PostMapping ;
import org.springframework.web.bind.annotation.RequestMapping ;
import org.springframework.web.bind.annotation.RequestMethod ;
import org.springframework.web.bind.annotation.RequestParam ;
import org.springframework.web.bind.annotation.RestController ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.client.FindIterable ;
import com.mongodb.util.JSON ;

@RestController
@CrossOrigin
@RequestMapping ( CsapEventsApplication.EVENT_API )
@CsapDoc ( title = "CSAP Events API" , type = CsapDoc.PUBLIC , notes = {
		"CSAP events apis provide access to query event records",
		"Used by both user sessions, AGENT use is deprecated but still on some older labs",
		"<a class='csap-link' target='_blank' href='https://github.com/csap-platform/csap-event-services'>learn more</a>",
		"<img class='csapDocImage' src='CSAP_BASE/images/event.png' />"
} )
public class ApiEventController {
	private static final int MONGO_DISABLED_INTERVAL_MS = 25000 ;

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;
	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	@Inject
	Environment env ;

	@Inject
	private EventDataReader eventDataReader ;

	@Inject
	private EventDataWriter eventDataWriter ;

	@Inject
	private EventDataHelper eventDataHelper ;

	@Inject
	private CsapEventsApplication csapEvents ;

	private volatile long lastMongoTimeoutError ;

	@CsapDoc ( notes = "Search on event" , linkTests = {
			"Search with appid etc",
			"lifecycle appid uiUser search"
	} , linkGetParams = {
			"search[value]='appId=csapeng.gen,eventReceivedOn=false,isDataRequired=false'",
			"search[value]='lifecycle=dev,appId=csapeng.gen,metaData.uiUser=paranant,project=Sample Release Package 2,eventReceivedOn=false,isDataRequired=false'"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@GetMapping ( produces = MediaType.APPLICATION_JSON_VALUE )
	public String getEventsPaginated (
										@RequestParam ( value = "length" , required = false , defaultValue = "20" ) Integer length ,
										@RequestParam ( value = "start" , required = false , defaultValue = "0" ) Integer start ,
										@RequestParam ( value = "search[value]" , required = false ) String searchValue ,
										@RequestParam ( value = "searchText" , required = false ) String searchText ) {

		logger.debug( "in event records {} , {} , {} , {}", length, start, searchValue, searchText ) ;

		var timer = metricUtilities.startTimer( ) ;

		String searchString = getSearchString( searchValue, searchText ) ;
		String data = "Error getting data" ;
		data = getPaginatedEvents( searchString, length, start ) ;
		metricUtilities.stopTimer( timer, "csap.event.get" ) ;

		return data ;

	}

	@CsapDoc ( notes = {
			"Post event record"
	} , //
			linkTests = {
					"Test1"
			} , //
			linkPostParams = { //
					"eventJson=FILE_eventInsert.json," + //
							SpringAuthCachingFilter.USERID + "=yourUserid," + //
							SpringAuthCachingFilter.PASSWORD + "=yourPassword"
			} )

	@PostMapping ( consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE )
	public String addEventRecord (
									@RequestParam ( value = "eventJson" , required = false ) String eventJson ,
									@RequestParam ( value = SpringAuthCachingFilter.USERID , required = false ) String userid ,
									@RequestParam ( value = SpringAuthCachingFilter.PASSWORD , required = false ) String pass ,
									HttpServletResponse response )
		throws Exception {

		//
		// csap agents send 30 second metrics
		// - 5 minute and 30 minute intervals are only sent once/day - unless agent is
		// restarted
		// - reason: less performance on both inserts and querys.
		//

		logger.debug( "Event post called.... {} , {} , {}", eventJson, userid, pass ) ;
		String result = "" ;

		if ( eventJson == null || userid == null || pass == null ) {

			metricUtilities.incrementCounter( "addEvent.attempt.failed.nullData" ) ;

			response.setStatus( HttpStatus.BAD_REQUEST.value( ) ) ;
			result = "Verify request content contains userid, password and event Data" ;

		} else if ( isMongoTimeoutError( ) ) {

			logger.error( "Mongo down not making call" ) ;
			response.setStatus( HttpStatus.SERVICE_UNAVAILABLE.value( ) ) ;
			result = EventDataWriter.MONGO_IS_TIMING_OUT ;
			metricUtilities.incrementCounter( "addEvent.attempt.failed.mongoDown" ) ;

		} else {

			metricUtilities.incrementCounter( "addEvent.attempt" ) ;
			result = "Added Event: " + eventDataWriter.insertEventData( eventJson, userid ) ;

			if ( EventDataWriter.MONGO_IS_TIMING_OUT.equalsIgnoreCase( result ) ) {

				metricUtilities.incrementCounter( "addEventRecord.mongoDown" ) ;
				response.setStatus( HttpStatus.SERVICE_UNAVAILABLE.value( ) ) ;
				lastMongoTimeoutError = System.currentTimeMillis( ) ;

			} else {

				logger.debug( "Mongo not down" ) ;
				lastMongoTimeoutError = 0 ;

			}

		}

		return result ;

	}

	@CsapDoc ( notes = "Count based on filter" , linkTests = {
			"Count based on fiter"
	} , linkGetParams = {
			"searchText='appId=csapeng.gen,eventReceivedOn=false,isDataRequired=false'"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@RequestMapping ( value = "/filteredCount" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Document getFilteredEventCount (
											@RequestParam ( value = "searchText" , required = false ) String searchString ) {

		var allTimer = metricUtilities.startTimer( ) ;

		var rowTimer = metricUtilities.startTimer( ) ;
		Long filteredCount = eventDataReader.numberOfEvents( searchString ) ;
		metricUtilities.stopTimer( rowTimer, "event.filtered-count.rowCount" ) ;

		var dataTimer = metricUtilities.startTimer( ) ;
		double dataSize = eventDataReader.getDataSize( ) ;
		metricUtilities.stopTimer( dataTimer, "event.filtered-count.dataSize" ) ;

		Document countResponse = new Document( ) ;
		countResponse.put( "recordsFiltered", filteredCount.longValue( ) ) ;

		if ( filteredCount.longValue( ) >= 0 ) {

			countResponse.put( "success", true ) ;

		}

		countResponse.put( "dataSize", dataSize ) ;

		metricUtilities.stopTimer( allTimer, "csap.event.filtered-count" ) ;

		logger.debug( "response: {} ", countResponse ) ;

		return countResponse ;

	}

	@CsapDoc ( notes = "Get data by id " , linkPaths = "/data/idhere" )
	@RequestMapping ( "/data/{id}" )
	public Document getData ( @PathVariable ( "id" ) String objectId ) {

		logger.debug( "Id {}", objectId ) ;
		return eventDataReader.getDataForObjectId( objectId ) ;

	}

	@CsapDoc ( notes = "Get latest event based on category. " , linkTests = {
			"category='/csap/health',appId=csapeng.gen,life=dev"
	} , linkGetParams = {
			"category='/csap/health',appId=csapeng.gen,life=dev"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@RequestMapping ( value = "/latest" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Document getLatest (
								@RequestParam ( value = "category" , required = false ) String category ,
								@RequestParam ( value = "appId" , required = false ) String appId ,
								@RequestParam ( value = "project" , required = false ) String project ,
								@RequestParam ( value = "keepMostRecent" , required = false , defaultValue = "-1" ) int keepMostRecent ,
								@RequestParam ( value = "life" , required = false ) String life ,
								@RequestParam ( value = "host" , required = false ) String host ) {

		return eventDataReader.getLatestEvent( category, appId, project, life, host, keepMostRecent ) ;

	}

	@CsapDoc ( notes = "Get latest event Json serilized " , linkTests = {
			"category='/csap/health',appId=csapeng.gen,life=dev",
			"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction"
	} , linkGetParams = {
			"category='/csap/health',appId=csapeng.gen,life=dev",
			"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE, "application/javascript"
	} )
	@RequestMapping ( value = "/latestWithJson" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Document getLatestEventWithJson (
												@RequestParam ( "category" ) String category ,
												@RequestParam ( "appId" ) String appId ,
												@RequestParam ( value = "project" , required = false ) String project ,
												@RequestParam ( value = "keepMostRecent" , required = false , defaultValue = "-1" ) int keepMostRecent ,
												@RequestParam ( "life" ) String life ) {

		return eventDataReader.getLatestEvent( category, appId, project, life, null, keepMostRecent ) ;

	}

	// @CsapDoc ( notes = "Count the records in db" , linkTests = {
	// "days=1,appId=csapeng.gen,life=dev,category='/csap/health'" } , linkGetParams
	// = {
	// "days=1,appId=csapeng.gen,life=dev,category='/csap/health'" } , produces = {
	// MediaType.APPLICATION_JSON_VALUE, "application/javascript" } )
	// @RequestMapping ( value = "/count" , produces =
	// MediaType.APPLICATION_JSON_VALUE )
	// public String countEvents (
	// @RequestParam ( value = "days" , required = false , defaultValue = "1" )
	// Integer days,
	// @RequestParam ( value = "appId" , required = false ) String appId,
	// @RequestParam ( value = "project" , required = false ) String project,
	// @RequestParam ( value = "life" , required = false ) String life,
	// @RequestParam ( value = "category" , required = false ) String category ) {
	// Split split = SimonManager.getStopwatch( "eventCount" ).start();
	// String result = "" + eventDataReader.countEvents( days, appId, life,
	// category, project );
	// logger.debug( "result {} ", result );
	// split.stop();
	// return result;
	// }

	@CsapDoc ( notes = "Count the records in db" , linkTests = {
			"callback=myTestFunction"
	} , linkGetParams = {
			"callback=myTestFunction"
	} , produces = {
			"application/javascript"
	} )
	@RequestMapping ( value = "/jcount" , produces = "application/javascript" )
	public String countEventsJsonp (
										@RequestParam ( value = "days" , required = false , defaultValue = "1" ) Integer days ,
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "life" , required = false ) String life ,
										@RequestParam ( value = "project" , required = false ) String project ,
										@RequestParam ( value = "category" , required = false ) String category ,
										@RequestParam ( value = "callback" , required = false , defaultValue = "false" ) String callback ) {

		var timer = metricUtilities.startTimer( ) ;
		String result = callback + "(" ;
		result = result + "{\"count\":" + eventDataReader.countEvents( days, appId, life, category, project ) + " } )" ;
		metricUtilities.stopTimer( timer, "eventJCount" ) ;
		return result ;

	}

	@CsapDoc ( notes = "Get meta data" , linkTests = {
			"appId=csapeng.gen,life=dev"
	} , linkGetParams = {
			"appId=csapeng.gen,life=dev"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@RequestMapping ( value = "/metadata" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String getEventMetaData (
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "life" , required = false ) String life ,
										@RequestParam ( value = "fromDate" , required = false ) String fromDate ,
										@RequestParam ( value = "toDate" , required = false ) String toDate ) {

		logger.debug( "Loading Meta" ) ;
		String result = "" ;

		try {

			EventMetaData metaData = eventDataReader.getEventMetaData( appId, life, fromDate, toDate, 2 ) ;
			result = jacksonMapper.writeValueAsString( metaData ) ;

		} catch ( Exception e ) {

			metricUtilities.incrementCounter( EventDataReader.SEARCH_FILTER_KEY + "errors" ) ;
			logger.error( "Exception while getting meta data", e ) ;

		}

		return result ;

	}

	@RequestMapping ( value = "/update" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String update (
							@RequestParam ( "eventData" ) String eventData ,
							@RequestParam ( "objectId" ) String objectId ) {

		logger.debug( "data {}", eventData ) ;
		logger.debug( "Object id {}", objectId ) ;
		int docsEffected = eventDataWriter.updateEvent( objectId, eventData ) ;
		return "{\"result\": \"Updated " + docsEffected + " record \"}" ;

	}

	@RequestMapping ( value = "/insert" , method = RequestMethod.POST , consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE )
	public String insertEventRecord (
										@RequestParam ( "eventJson" ) String eventJson ,
										@RequestParam ( "appId" ) String appId ,
										@RequestParam ( "life" ) String life ,
										@RequestParam ( "project" ) String project ,
										@RequestParam ( "summary" ) String summary ,
										@RequestParam ( "category" ) String category )
		throws Exception {

		var timer = metricUtilities.startTimer( ) ;
		logger.debug( "event json --> {} appId {} life {} project {} summary {},category {}", eventJson, appId, life,
				project, summary, category ) ;
		String result = eventDataWriter.insertEvent( eventJson, appId, project, life, summary, category ) ;
		logger.debug( "Inserted objid {} ", result ) ;
		String output = "" ;

		if ( result.equalsIgnoreCase( "Invalid data" ) ) {

			output = "{\"result\": \"Error inserting. " + result + "\"}" ;

		} else {

			output = "{\"result\": \"Inserted objid " + result + "\"}" ;

		}

		metricUtilities.stopTimer( timer, "csap.eventInsert" ) ;
		return output ;

	}

	@CsapDoc ( notes = "Delete using serach criteria " , linkPaths = "/removeThis/deleteBySearch" , linkGetParams = {
			"search[value]='lifecycle=dev,appId=csapeng.gen,metaData.uiUser=testUser,project=Sample Release Package 2'"
	} )
	@RequestMapping ( value = "/deleteBySearch" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String deleteBySearchString ( @RequestParam ( "searchString" ) String searchString ) {

		long docsEffected = eventDataWriter.deleteEventByFilter( searchString ) ;
		return "{\"result\": \"Deleted " + docsEffected + " record \"}" ;

	}

	@CsapDoc ( notes = "Delete by object id " , linkPaths = "/removeThis/delete" , linkGetParams = {
			"objectId=objIdHere"
	} )
	@RequestMapping ( value = "/delete" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String delete ( @RequestParam ( "objectId" ) String objectId ) {

		long docsEffected = eventDataWriter.deleteEventById( objectId ) ;
		logger.debug( "object id {}", objectId ) ;
		return "{\"result\": \"Deleted " + docsEffected + " record \"}" ;

	}

	@CsapDoc ( notes = "Retrieve log rotate report" , linkTests = {
			"appId=csapeng.gen,life=dev",
			"callback=myTestFunction"
	} , linkGetParams = {
			"appId=csapeng.gen,life=dev",
			"appId=csapeng.gen,life=dev,callback=myTestFunction"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@RequestMapping ( value = "/logRotateReport" , produces = MediaType.APPLICATION_JSON_VALUE )
	public JsonNode retrieveCustomLogRotateReport (
													@RequestParam ( value = "category" , required = false , defaultValue = "/csap/reports/logRotate" ) String category ,
													@RequestParam ( value = "appId" , required = false ) String appId ,
													@RequestParam ( value = "life" , required = false ) String life ,
													@RequestParam ( value = "project" , required = false ) String project ,
													@RequestParam ( value = "fromDate" , required = false ) String fromDate ,
													@RequestParam ( value = "toDate" , required = false ) String toDate ) {

		return eventDataReader.retrieveLogRotateReport( appId, project, life, category, fromDate, toDate ) ;

	}

	@CsapDoc ( notes = "Get event by id " )
	@RequestMapping ( value = "/getById" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Document getDataById ( @RequestParam ( value = "id" , required = true ) String objectId ) {

		logger.debug( "Id {}", objectId ) ;
		return eventDataReader.getEventByObjectId( objectId ) ;

	}

	@CsapDoc ( notes = {
			"mongo db.serverStatus() command output "
	} )
	@RequestMapping ( value = "/serverStatus" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Document serverStatus ( )
		throws Exception {

		return eventDataHelper.serverStatus( ) ;

	}

	@CsapDoc ( notes = "Get latest event based on category. " , linkTests = {
			"category='/csap/health',appId=csapeng.gen,life=dev",
			"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction"
	} , linkGetParams = {
			"category='/csap/health',appId=csapeng.gen,life=dev",
			"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE, "application/javascript"
	} )
	@RequestMapping ( value = "/latestCached" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Document getLatestCached (
										@RequestParam ( "category" ) String category ,
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "project" , required = false ) String project ,
										@RequestParam ( value = "keepMostRecent" , required = false , defaultValue = "-1" ) int keepMostRecent ,
										@RequestParam ( value = "life" , required = false ) String life ) {

		Document latestEvent = eventDataReader.getLatestCachedEvent( category, appId, project, life, keepMostRecent ) ;
		return latestEvent ;

	}

	@CsapDoc ( notes = "Get app ids " , linkTests = {
			"days=4,category='/csap/health'", "callback=myTestFunction"
	} , linkGetParams = {
			"numDays=4,category='/csap/health'",
			"callback=myTestFunction"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE, "application/javascript"
	} )
	@RequestMapping ( value = "/appIds" , produces = MediaType.APPLICATION_JSON_VALUE )
	public List<Document> getAppIds (
										@RequestParam ( value = "numDays" , required = false , defaultValue = "7" ) int numDays ,
										@RequestParam ( value = "category" , required = false ) String category ) {

		return eventDataReader.getAppIdProject( numDays, category ) ;

	}

	@CsapDoc ( notes = "Get lifecycle for app ids" , linkTests = {
			"appId=csapeng.gen,numDays=4,category='/csap/health'",
			"appId=csapeng.gen,callback=myTestFunction"
	} , linkGetParams = {
			"appId=csapeng.gen,numDays=4,category='/csap/health'",
			"appId=csapeng.gen,callback=myTestFunction"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE,
			"application/javascript"
	} )
	@RequestMapping ( value = "/lifecycles" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Document getLifeForAppId (
										@RequestParam ( "appId" ) String appId ,
										@RequestParam ( value = "numDays" , required = false , defaultValue = "7" ) int numDays ,
										@RequestParam ( value = "category" , required = false ) String category ) {

		return eventDataReader.getLifecycles( appId, numDays, category ) ;

	}

	@CsapDoc ( notes = "Count the records in db" , linkTests = {
			"days=1,appId=csapeng.gen,life=dev,category='/csap/health'",
			"callback=myTestFunction"
	} , linkGetParams = {
			"days=1,appId=csapeng.gen,life=dev,category='/csap/health'",
			"callback=myTestFunction"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE, "application/javascript"
	} )
	@RequestMapping ( value = "/count" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode countEvents (
									@RequestParam ( value = "days" , required = false , defaultValue = "1" ) Integer days ,
									@RequestParam ( value = "appId" , required = false ) String appId ,
									@RequestParam ( value = "project" , required = false ) String project ,
									@RequestParam ( value = "life" , required = false ) String life ,
									@RequestParam ( value = "category" , required = false ) String category ) {

		var timer = metricUtilities.startTimer( ) ;
		long result = eventDataReader.countEvents( days, appId, life, category, project ) ;
		ObjectNode node = jacksonMapper.createObjectNode( ) ;
		node.put( "count", result ) ;
		metricUtilities.stopTimer( timer, "csap.eventApiCount" ) ;
		return node ;

	}

	@Autowired
	HealthEventWriter healthEventWriter ;

	@CsapDoc ( notes = "Report of discovered applications" )
	@GetMapping ( value = "/discovery" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode discovery ( ) {

		return healthEventWriter.getDiscoveryReport( ) ;

	}

	private String getPaginatedEvents ( String searchString , int length , int startIndex ) {

		String json = "" ;
		String data = "" ;

		ObjectNode rootNode = jacksonMapper.createObjectNode( ) ;
		// try (DBCursor cursor =
		// eventDataReader.getFilteredEvents(searchString,length,startIndex)) {
		FindIterable<Document> cursor = eventDataReader.getEventsByCriteria( searchString, length, startIndex ) ;
		data = "\"data\": " + JSON.serialize( cursor ) + "" ;

		try {

			rootNode.put( "aaData", "value" ) ;
			rootNode.put( EventJsonConstants.DATA_TABLES_RECORDS_TOTAL, eventDataReader.getTotalEvents(
					searchString ) ) ;
			// rootNode.put("recordsFiltered",
			// eventDataReader.getTotalFilteredEvents(searchString));
			// event count can take a while - hard code a dummy value
			rootNode.put( EventJsonConstants.DATA_TABLES_RECORDS_FILTERED, -1 ) ;
			json = jacksonMapper.writeValueAsString( rootNode ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while converting json", e ) ;

		}

		json = json.replace( "\"aaData\":\"value\"", data ) ;
		return json ;

	}

	private String getSearchString ( String searchValue , String searchText ) {

		String searchString = "" ;

		if ( null != searchValue && searchValue.trim( ).length( ) > 0 ) {

			searchString = searchValue ;

		} else if ( null != searchText && searchText.trim( ).length( ) > 0 ) {

			searchString = searchText ;

		}

		return searchString ;

	}

	// Avoid slamming mongo - wait between intervals
	private boolean isMongoTimeoutError ( ) {

		long currentTime = System.currentTimeMillis( ) ;
		logger.debug( "diff {} ", ( currentTime - lastMongoTimeoutError ) ) ;

		if ( lastMongoTimeoutError == 0 ) {

			logger.debug( "lastMongoTimeoutError " + lastMongoTimeoutError ) ;
			return false ;

		} else if ( ( currentTime - lastMongoTimeoutError ) < MONGO_DISABLED_INTERVAL_MS ) {

			return true ;

		} else {

			return false ;

		}

	}
}
