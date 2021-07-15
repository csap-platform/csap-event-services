package org.csap.events.db ;

import static org.csap.events.EventJsonConstants.CATEGORY ;

import java.time.LocalDate ;
import java.time.LocalDateTime ;
import java.time.format.DateTimeFormatter ;
import java.util.ArrayList ;
import java.util.Calendar ;
import java.util.List ;
import java.util.concurrent.TimeUnit ;
import java.util.concurrent.locks.Lock ;
import java.util.concurrent.locks.ReentrantLock ;

import javax.inject.Inject ;

import org.apache.commons.lang3.StringUtils ;
import org.bson.Document ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.util.DateUtil ;
import org.csap.events.util.MetricsJsonConstants ;
import org.csap.helpers.CSAP ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.scheduling.annotation.Async ;
import org.springframework.stereotype.Component ;

import com.fasterxml.jackson.core.JsonProcessingException ;
import com.fasterxml.jackson.databind.JsonMappingException ;
import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.client.model.FindOneAndUpdateOptions ;
import com.mongodb.client.model.ReturnDocument ;

@Component
public class HealthEventWriter {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Inject
	private CsapEventsApplication eventsApp ;

	@Inject
	private EventDataHelper eventDataHelper ;

	ObjectMapper jsonMapper = new ObjectMapper( ) ;

	volatile ObjectNode discoveryReport ;

	volatile int lastClearedDay = -1 ;
	Lock discoveryLock = new ReentrantLock( ) ;

	public HealthEventWriter ( ) {

		discoveryReport = jsonMapper.createObjectNode( ) ;

	}

	// CANNOT be invoked from same class unless AspectJ configured. Invoked from
	// Landing Page
	@Async ( CsapEventsApplication.HEALTH_EXECUTOR )
	public void printMessage ( String message , int delaySeconds )
		throws Exception {

		Thread.sleep( 5000 ) ;
		logger.info( "Time now: {}, Message Received: {}",
				LocalDateTime.now( ).format( DateTimeFormatter.ofPattern( "hh:mm:ss" ) ),
				message ) ;

	}

	@Async ( CsapEventsApplication.HEALTH_EXECUTOR )
	public void writeHealthEvent (
									Document eventDocument ,
									String eventJson ,
									String appId ) {

		metricUtilities.incrementCounter( "db-event.insert.health" ) ;
		var projectName = eventDocument.getString( "project" ) ;
		var life = eventDocument.getString( "lifecycle" ) ;
		var host = eventDocument.getString( "host" ) ;

		try {

			// boolean isHealthReportEnabled =
			// eventDataHelper.isHealthReportEnabled(projectName, life);
			logger.debug( "checking project {} life {} ", projectName, life ) ;
			boolean isHealthReportEnabled = eventDataHelper.isSaveHealthReport( projectName, life ) ;
			logger.debug( "Health event enabled: {},  for project {} life {} ",
					projectName, life, isHealthReportEnabled ) ;

			if ( isHealthReportEnabled ) {

				performHealthProcessing( eventDocument ) ;

			} else {

				logger.debug( "Health report not enabled for {} and life {} ", projectName, life ) ;

			}

			if ( discoveryLock.tryLock( ) ) {

				try {

					var fullEvent = jsonMapper.readTree( eventJson ) ;
					addDiscoveryHost( host, appId, projectName, life, fullEvent.path( "data" ) ) ;

				} catch ( Exception e ) {

					logger.warn( "{} Failed discovery: {}, project: {}, environment: {}",
							host, appId, projectName, life, CSAP.buildCsapStack( e ) ) ;

				} finally {

					discoveryLock.unlock( ) ;

				}

			} else {

				logger.info( "Skipping discovery: {} due to lock in use, disovery on next try", host ) ;

			}

		} catch ( Exception e ) {

			logger.warn( "{} life {} Failed inserting health event:\n {} {}",
					projectName, life, eventDocument, CSAP.buildCsapStack( e ) ) ;

		}

	}

	public void addDiscoveryHost ( String host , String appId , String projectName , String life , JsonNode data )
		throws JsonProcessingException ,
		JsonMappingException {

		// clear cache every night
		int nowDayOfYear = LocalDate.now( ).getDayOfYear( ) ;

		if ( nowDayOfYear != lastClearedDay ) {

			lastClearedDay = nowDayOfYear ;
			logger.info( "Clearing discovered hosts" ) ;
			discoveryReport = jsonMapper.createObjectNode( ) ;

		}

		var appIdReport = discoveryReport.path( appId ) ;

		if ( ! appIdReport.isObject( ) ) {

			discoveryReport.putObject( appId ) ;

		}

		var appIdObj = (ObjectNode) discoveryReport.path( appId ) ;

		var vmReport = data.path( "vm" ) ;
		var loadbalancer = vmReport.path( MetricsJsonConstants.LOADBALANCER_URL ).asText( ) ;

		if ( StringUtils.isNotEmpty( loadbalancer ) ) {

			appIdObj.put( life, loadbalancer ) ;

		} else {

			appIdObj.put( life, host ) ;

		}

		logger.debug( "{} Adding: appid {}, lb: {} project: {}, environment: {}", host, appId, loadbalancer,
				projectName, life ) ;

	}

	public void performHealthProcessing ( Document eventDocument ) {

		// logger.info("Inside write health event");
		eventDocument.put( CATEGORY, "/csap/reports/health" ) ;
		Document query = eventDataHelper.constructCategoryHostDateDocumentQuery( eventDocument ) ;
		Document healthObject = eventDataHelper.getMongoEventCollection( )
				.find( query )
				.maxTime( 1, TimeUnit.SECONDS )
				.limit( 1 )
				.first( ) ;

		if ( null == healthObject ) {

			var timer = metricUtilities.startTimer( ) ;
			logger.debug( "Health object does not exists" ) ;
			constructDataObject( eventDocument ) ;
			logger.debug( "Health object" + eventDocument ) ;
			updateOneRecordPerDay( eventDocument ) ;
			metricUtilities.stopTimer( timer, "db-event.insert.health.new" ) ;

		} else {

			var timer = metricUtilities.startTimer( ) ;
			logger.debug( "Updating health report for host: {} ", eventDocument.getString( "host" ) ) ;

			boolean isHealthStatusChanged = isHealthStatusChanged( eventDocument, healthObject ) ;
			logger.debug( "isHealthStatusChanged: {} ", isHealthStatusChanged ) ;

			boolean isHealthStatusArrayFull = isHealthStatusArrayFull( healthObject ) ;
			logger.debug( "isHealthStatusArrayFull: {} ", isHealthStatusArrayFull ) ;

			boolean isUnHealthyEventExists = isUnHealthyEventExists( eventDocument ) ;
			logger.debug( "isUnHealthyEventExists: {} ", isUnHealthyEventExists ) ;

			if ( isUnHealthyEventExists ) {

				Document healthDataObject = (Document) healthObject.get( "data" ) ;
				int unHealthyEventCount = (Integer) healthDataObject.get( "UnHealthyEventCount" ) ;
				unHealthyEventCount = unHealthyEventCount + 1 ;
				healthDataObject.put( "UnHealthyEventCount", unHealthyEventCount ) ;

			}

			if ( isHealthStatusChanged && ! isHealthStatusArrayFull ) {

				// Update counter and error message
				addToHealthStatus( eventDocument, healthObject ) ;
				updateOneRecordPerDay( healthObject ) ;

			} else {

				// Update only the counter
				if ( isUnHealthyEventExists ) {

					healthObject.remove( "counter" ) ;
					updateOneRecordPerDay( healthObject ) ;

				}

			}

			metricUtilities.stopTimer( timer, "db-event.insert.health.update" ) ;

		}

	}

	private boolean isUnHealthyEventExists ( Document eventDBObject ) {

		Document dataObject = (Document) eventDBObject.get( "data" ) ;
		Document errorsObject = (Document) dataObject.get( "errors" ) ;

		if ( null != errorsObject ) {

			return true ;

		}

		return false ;

	}

	private boolean isHealthStatusChanged ( Document eventDBObject , Document healthObject ) {

		Document dataObject = (Document) eventDBObject.get( "data" ) ;
		Document errorsObject = (Document) dataObject.get( "errors" ) ;
		String hostName = eventDBObject.getString( "host" ) ;
		int currentHealthStatusLength = 0 ;

		if ( null != errorsObject ) {

			List errorsList = (List) errorsObject.get( hostName ) ;
			String currentHealthStatus = errorsList.toString( ).trim( ) ;
			currentHealthStatusLength = currentHealthStatus.length( ) ;

		} else {

			List currentStatusList = new ArrayList( ) ;
			currentStatusList.add( "Success" ) ;
			currentHealthStatusLength = currentStatusList.toString( ).length( ) ;

		}

		Document healthDataObject = (Document) healthObject.get( "data" ) ;
		List healthStatusList = (List) healthDataObject.get( "healthStatus" ) ;
		Document lastHealthObj = (Document) healthStatusList.get( ( healthStatusList.size( ) - 1 ) ) ;
		List lastHealthStatus = (List) lastHealthObj.get( "status" ) ;
		int lastHealthStatusLength = lastHealthStatus.toString( ).length( ) ;
		return currentHealthStatusLength != lastHealthStatusLength ;

	}

	private boolean isHealthStatusArrayFull ( Document healthObject ) {

		Document healthDataObject = (Document) healthObject.get( "data" ) ;
		List healthStatusList = (List) healthDataObject.get( "healthStatus" ) ;
		logger.debug( "health staus size {}", healthStatusList.size( ) ) ;
		return healthStatusList.size( ) > eventsApp.getMaxHealthChangesPerDay( ) ;

	}

	private void addToHealthStatus ( Document eventDBObject , Document healthObject ) {

		healthObject.remove( "counter" ) ;
		Document dataObject = (Document) eventDBObject.get( "data" ) ;
		Document errorsObject = (Document) dataObject.get( "errors" ) ;
		String hostName = eventDBObject.getString( "host" ) ;
		List currentStatusList = null ;
		Document healthDataObject = (Document) healthObject.get( "data" ) ;

		if ( null != errorsObject ) {

			currentStatusList = (List) errorsObject.get( hostName ) ;

			// int unHealthyEventCount =
			// (Integer)healthDataObject.get("UnHealthyEventCount");
			// unHealthyEventCount++;
			// healthDataObject.put("UnHealthyEventCount", unHealthyEventCount);
			// logger.info("health data object {}",healthDataObject);
		} else {

			currentStatusList = new ArrayList( ) ;
			currentStatusList.add( "Success" ) ;

		}

		List healthStatusList = (List) healthDataObject.get( "healthStatus" ) ;
		Document newHealthStatus = new Document( ) ;
		newHealthStatus.append( "time", DateUtil.getFormatedTime( Calendar.getInstance( ) ) ) ;
		newHealthStatus.append( "status", currentStatusList ) ;
		healthStatusList.add( newHealthStatus ) ;

	}

	private String updateOneRecordPerDay ( Document eventDBObject ) {

		String key = "Failure" ;
		Document query = eventDataHelper.constructCategoryHostDateDocumentQuery( eventDBObject ) ;
		Document setObject = new Document( ) ;
		setObject.put( "$set", eventDBObject ) ;
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions( ) ;
		options.upsert( true ) ;
		options.returnDocument( ReturnDocument.AFTER ) ;

		// DBCollection eventCollection = eventDataHelper.getEventCollection();
		try {

			// eventCollection.update(query, setObject, true, false);
			Document result = eventDataHelper.getMongoEventCollection( ).findOneAndUpdate( query, setObject, options ) ;

			if ( null != result ) {

				key = result.getObjectId( "_id" ).toString( ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while inserting health metrics", e ) ;

		}

		return key ;

	}

	private void constructDataObject ( Document eventDBObject ) {

		eventDBObject.remove( "counter" ) ;
		Document dataObject = (Document) eventDBObject.get( "data" ) ;
		Document errorObject = constructErrorObject( eventDBObject ) ;

		if ( (Boolean) dataObject.get( "Healthy" ) ) {

			dataObject.put( "UnHealthyEventCount", 0 ) ;

		} else {

			dataObject.put( "UnHealthyEventCount", 1 ) ;

		}

		dataObject.remove( "Healthy" ) ;
		dataObject.remove( "errors" ) ;
		List errorList = new ArrayList( ) ;
		errorList.add( errorObject ) ;
		dataObject.put( "healthStatus", errorList ) ;

	}

	private Document constructErrorObject ( Document eventDBObject ) {

		Document dataObject = (Document) eventDBObject.get( "data" ) ;
		Document vmObject = (Document) dataObject.get( "vm" ) ;
		Document servicesObject = (Document) dataObject.get( "services" ) ;
		Document errorsObject = (Document) dataObject.get( "errors" ) ;
		String hostName = eventDBObject.getString( "host" ) ;
		Document errorObj = new Document( ) ;
		errorObj.append( "time", DateUtil.getFormatedTime( Calendar.getInstance( ) ) ) ;

		if ( null != errorsObject ) {

			List errorsList = (List) errorsObject.get( hostName ) ;
			String errorAsString = errorsList.toString( ) ;
			errorObj.append( "status", errorsList ) ;

		} else {

			List errorsList = new ArrayList( ) ;
			errorsList.add( "Success" ) ;
			errorObj.append( "status", errorsList ) ;

		}

		return errorObj ;

	}

	public ObjectNode getDiscoveryReport ( ) {

		return discoveryReport ;

	}

	public void setDiscoveryReport ( ObjectNode discoveryReport ) {

		this.discoveryReport = discoveryReport ;

	}

}
