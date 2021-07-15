package org.csap.events.db ;

import static com.mongodb.client.model.Filters.and ;
import static com.mongodb.client.model.Filters.eq ;
import static org.csap.events.EventJsonConstants.APPID ;
import static org.csap.events.EventJsonConstants.CATEGORY ;
import static org.csap.events.EventJsonConstants.CREATED_ON ;
import static org.csap.events.EventJsonConstants.DATA ;
import static org.csap.events.EventJsonConstants.DATE ;
import static org.csap.events.EventJsonConstants.HOST ;
import static org.csap.events.EventJsonConstants.LAST_UPDATED_ON ;
import static org.csap.events.EventJsonConstants.LIFE ;
import static org.csap.events.EventJsonConstants.PAYLOAD_CATEGORY ;
import static org.csap.events.EventJsonConstants.PROJECT ;
import static org.csap.events.EventJsonConstants.SUMMARY ;
import static org.csap.events.EventJsonConstants.TIME ;
import static org.csap.events.EventJsonConstants.UNIXMS ;
import static org.csap.events.util.MongoConstants.EVENT_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.EVENT_DB_NAME ;

import java.net.InetAddress ;
import java.net.UnknownHostException ;
import java.util.ArrayList ;
import java.util.Calendar ;
import java.util.Date ;
import java.util.List ;
import java.util.concurrent.TimeUnit ;

import javax.inject.Inject ;

import org.apache.commons.lang3.StringUtils ;
import org.bson.Document ;
import org.bson.conversions.Bson ;
import org.bson.types.ObjectId ;
import org.csap.events.util.DateUtil ;
import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapApplication ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.stereotype.Service ;

import com.mongodb.BasicDBObject ;
import com.mongodb.DBCollection ;
import com.mongodb.DBObject ;
import com.mongodb.MongoClient ;
import com.mongodb.MongoTimeoutException ;
import com.mongodb.WriteResult ;
import com.mongodb.client.MongoCollection ;
import com.mongodb.client.model.FindOneAndUpdateOptions ;
import com.mongodb.client.model.ReturnDocument ;
import com.mongodb.client.result.DeleteResult ;
import com.mongodb.util.JSON ;

@Service
public class EventDataWriter {
	public static final String MONGO_IS_TIMING_OUT = "Mongo Not Available" ;
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;
	@Inject
	private MongoClient mongoClient ;

	@Inject
	private EventDataHelper eventDataHelper ;

	@Inject
	private HealthEventWriter healthEventWriter ;

	public long deleteEventByFilter ( String filter ) {

		long docsEffected = 0 ;
		Bson query = eventDataHelper.convertUserInterfaceQueryToMongoFilter( filter ) ;
		DeleteResult result = eventDataHelper.getMongoEventCollection( ).deleteMany( query ) ;

		if ( null != result ) {

			docsEffected = result.getDeletedCount( ) ;

		}

		logger.warn( "Docs effected {} ", docsEffected ) ;
		return docsEffected ;

	}

	public long deleteEventById ( String objectId ) {

		Document query = new Document( "_id", new ObjectId( objectId ) ) ;
		DeleteResult result = eventDataHelper.getMongoEventCollection( ).deleteOne( query ) ;
		long docsEffected = 0 ;

		if ( null != result ) {

			docsEffected = result.getDeletedCount( ) ;
			logger.warn( "docs effected {}", docsEffected ) ;

		}

		return docsEffected ;

	}

	public int updateEvent ( String objectId , String eventData ) {

		// DBObject eventDataObject = (DBObject) JSON.parse(eventData);
		DBObject query = new BasicDBObject( "_id", new ObjectId( objectId ) ) ;
		DBCollection eventCollection = getEventCollection( ) ;
		BasicDBObject dbObject = (BasicDBObject) eventCollection.findOne( query ) ;
		BasicDBObject dataObject = (BasicDBObject) dbObject.get( "data" ) ;
		dataObject.put( "csapText", eventData ) ;
		// dbObject.put("data", eventDataObject);
		BasicDBObject createdOn = (BasicDBObject) dbObject.get( CREATED_ON ) ;
		Calendar calendar = Calendar.getInstance( ) ;
		createdOn.put( LAST_UPDATED_ON, calendar.getTime( ) ) ;
		// setExpiresAt(dbObject);
		WriteResult result = eventCollection.update( query, dbObject ) ;
		int docsEffected = 0 ;

		if ( null != result ) {

			docsEffected = result.getN( ) ;
			logger.debug( "docs effected {}", docsEffected ) ;

		}

		return docsEffected ;

	}

	public String insertEvent (
								String eventJson ,
								String appId ,
								String project ,
								String life ,
								String summary ,
								String category ) {

		String key = "" ;

		if ( StringUtils.isBlank( eventJson ) ||
				StringUtils.isBlank( appId ) ||
				StringUtils.isBlank( project ) ||
				StringUtils.isBlank( life ) ||
				StringUtils.isBlank( summary ) ||
				StringUtils.isBlank( category ) ) {

			logger.error( "Invalid data eventData {} appId {} project {} life {} ", eventJson, appId, project, life ) ;
			return "Insufficient data" ;

		} else {

			try {

				Document eventDocument = new Document( ) ;
				eventDocument.put( APPID, appId ) ;
				eventDocument.put( LIFE, life ) ;
				eventDocument.put( PROJECT, project ) ;
				eventDocument.put( HOST, getHostName( ) ) ;
				eventDocument.put( CATEGORY, category ) ;
				eventDocument.put( SUMMARY, summary ) ;
				setMongoDate( eventDocument ) ;

				if ( ! category.startsWith( "/csap/settings" ) ) {

					eventDocument.append( "expiresAt", getExpirationTime( eventDocument.getString( "lifecycle" ) ) ) ;

				}

				if ( eventJson.startsWith( "{" ) || eventJson.startsWith( "[" ) ) {

					eventDocument.put( "data", JSON.parse( eventJson ) ) ;

				} else {

					Document data = new Document( ) ;
					data.put( "csapText", eventJson ) ;
					eventDocument.put( "data", data ) ;

				}

				eventDataHelper.getMongoEventCollection( ).insertOne( eventDocument ) ;
				key = eventDocument.get( "_id" ).toString( ) ;

			} catch ( Exception e ) {

				logger.error( "Exception while inserting data ", e ) ;
				key = "Invalid data" ;

			}

		}

		return key ;

	}

	/**
	 * This will insert events and/or metrics based on category.
	 * 
	 * @param eventJson
	 * @param appId
	 * @return
	 */
	public String insertEventData ( String eventJson , String appId ) {

		String documentKeyUsedInLogs = "" ;

		if ( StringUtils.isBlank( eventJson ) ) {

			documentKeyUsedInLogs = "EventJson Empty" ;
			logger.error( "APP id when event json is null {}", appId ) ;
			addExceptionEvent( "Empty Event json", eventJson, appId ) ;
			return documentKeyUsedInLogs ;

		}

		var allTimer = metricUtilities.startTimer( ) ;

		try {

			Document eventDocument = Document.parse( eventJson ) ;
			eventDocument.append( "appId", appId ) ;
			setMongoDate( eventDocument ) ;
			String category = eventDocument.getString( CATEGORY ) ;

			logger.debug( "Inserting type: {}", category ) ;

			if ( StringUtils.isNotEmpty( category ) ) {

				if ( ! category.startsWith( "/csap/settings" ) ) {

					eventDocument.append( "expiresAt", getExpirationTime( eventDocument.getString( "lifecycle" ) ) ) ;

				}

				var categoryTimer = metricUtilities.startTimer( ) ;

				if ( category.startsWith( "/csap/metrics" ) ) {

					documentKeyUsedInLogs = insertPerformanceDocument( eventDocument, appId ) ;

				} else if ( category.startsWith( "/csap/health" ) ) {

					logger.debug( "Troubleshooting: health updates" ) ;

					documentKeyUsedInLogs = insertOrUpdate( eventDocument ) ;

					try {

						healthEventWriter.writeHealthEvent( eventDocument, eventJson, appId ) ;

					} catch ( Exception e ) {

						metricUtilities.incrementCounter( "db-event.attempt.failed." + e.getClass( ).getName( ) ) ;
						logger.error( "Exception while writing health report event", e ) ;

					}

				} else if ( "/csap/system/memory/low".equalsIgnoreCase( category ) ) {

					documentKeyUsedInLogs = insertOrUpdate( eventDocument ) ;

				} else if ( category.startsWith( "/csap/ui/access/" )
						&& ! category.startsWith( "/csap/ui/access/begin" )
						&& ! category.startsWith( "/csap/ui/access/end" ) ) {

					//
					// CSAP Access list: /access : used for host sessions - not upserted
					// begin/end tracks user session times - not upserted
					// /csap/ui/access/<userid> - ONLY this is upserted
					//
					documentKeyUsedInLogs = insertOrUpdate( eventDocument ) ;

				} else if ( category.startsWith( "/csap/reports" ) ) {

					documentKeyUsedInLogs = insertOrUpdate( eventDocument ) ;

				} else if ( category.matches( "/csap/system/service/.*/job" ) ) {

					documentKeyUsedInLogs = insertOrUpdate( eventDocument ) ;

				} else {

					eventDataHelper.getMongoEventCollection( )
							.insertOne( eventDocument ) ;
					documentKeyUsedInLogs = eventDocument.get( "_id" ).toString( ) ;

				}

				metricUtilities.stopTimer( categoryTimer, "csap.event.add." + timerName( category ) ) ;

			} else {

				logger.error( "category is null. Not inserting data" ) ;

			}

		} catch ( Exception e ) {

			metricUtilities.incrementCounter( "db-event.attempt.failed." + e.getClass( ).getName( ) ) ;

			if ( e instanceof MongoTimeoutException ) {

				documentKeyUsedInLogs = MONGO_IS_TIMING_OUT ;

			} else {

				logger.error( eventJson ) ;
				documentKeyUsedInLogs = "Failure" ;
				addExceptionEvent( e.getMessage( ), eventJson, appId ) ;

			}

			logger.error( "Exception while inserting event record {}", CSAP.buildCsapStack( e ) ) ;

		}

		metricUtilities.stopTimer( allTimer, "csap.event.add" ) ;
		return documentKeyUsedInLogs ;

	}

	private String timerName ( String category ) {

		String name = "" ;

		if ( category.startsWith( "/csap/metrics" ) && category.endsWith( "data" ) ) {

			name = "metrics-data" ;

		} else if ( category.startsWith( "/csap/metrics" ) && category.endsWith( "attributes" ) ) {

			name = "metrics-attributes" ;

		} else if ( category.startsWith( "/csap/health" ) ) {

			name = "health" ;

		} else if ( "/csap/system/memory/low".equalsIgnoreCase( category ) ) {

			name = "low-memory" ;

		} else if ( category.startsWith( "/csap/reports" ) ) {

			name = "summary-report" ;

		} else if ( category.startsWith( "/csap/ui" ) ) {

			name = "user-activity" ;

		} else if ( category.startsWith( "/csap/system" ) ) {

			name = "csap-system" ;

		} else {

			name = "misc" ;

		}

		return CSAP.camelToSnake( name ) ;

	}

	private Date getExpirationTime ( String life ) {

		Calendar calendar = Calendar.getInstance( ) ;

		if ( "prod".equalsIgnoreCase( life ) ) {

			calendar.add( Calendar.MONTH, 24 ) ;

		} else {

			calendar.add( Calendar.MONTH, 12 ) ;

		}

		return calendar.getTime( ) ;

	}

	private String insertPerformanceDocument ( Document eventDocument , String appId ) {

		//
		// either an attributes or data event:
		// - step 1: both are stored in the metrics db
		// - step 2: an event is added to event db, including a key used to xref the
		// data.
		//

		String key = "" ;
		String category = eventDocument.getString( "category" ) ;
		MongoCollection<Document> metricsAttributeOrDataCollection = eventDataHelper.getMetricsCollectionByCategory(
				category ) ;
		String metricsDocumentIdUsedForCrossReferences = "" ;

		if ( null != metricsAttributeOrDataCollection ) {

			Document dataDocument = (Document) eventDocument.get( "data" ) ;
			dataDocument.append( "createdOn", eventDocument.get( "createdOn" ) ) ;
			metricsAttributeOrDataCollection.insertOne( dataDocument ) ;
			metricsDocumentIdUsedForCrossReferences = dataDocument.get( "_id" ).toString( ) ;

			logger.debug( "Metrics Key {}", metricsDocumentIdUsedForCrossReferences ) ;

		}

		//
		// data is stored ONLY in the metrics db; a xref key is used for track
		// relationship/browse the data in ui
		//
		eventDocument.remove( "data" ) ;

		if ( StringUtils.isNotBlank( metricsDocumentIdUsedForCrossReferences ) ) {

			eventDocument.append( "dataKey", metricsDocumentIdUsedForCrossReferences ) ;

		}

		if ( category.startsWith( "/csap/metrics" ) && category.endsWith( "attributes" ) ) {

			eventDataHelper.getMongoEventCollection( ).insertOne( eventDocument ) ;
			key = eventDocument.get( "_id" ).toString( ) ;

		} else if ( category.startsWith( "/csap/metrics" ) && category.endsWith( "data" ) ) {

			key = insertOrUpdate( eventDocument ) ;

		} else {

			logger.debug( "Category not ending with data or attribute :: {}", category ) ;
			metricUtilities.incrementCounter( "db-event.attempt.failed.eol.format." + appId ) ;

		}

		return key ;

	}

	private String insertOrUpdate ( Document eventDocument ) {

		var eventCategory = eventDocument.getString( "category" ) ;

		var timer = metricUtilities.startTimer( ) ;

		String key = "" ;

		List<Bson> queryFilters = new ArrayList<>( ) ;
		// queryFilters.add(eq("lifecycle",eventDocument.getString("lifecycle")));
		queryFilters.add(
				eq( "category", eventCategory ) ) ;

		// Special hook for analytics project generating one report for each
		// deployed instance
		// /csap/ui/access
		// if ( ! (("/csap/reports/global/daily").equalsIgnoreCase( eventCategory )) ) {
		if ( eventCategory.equalsIgnoreCase( "/csap/reports/global/daily" )
				|| eventCategory.startsWith( "/csap/ui/access/" ) ) {

			// ignore host
		} else {

			queryFilters.add( eq( "host", eventDocument.getString( "host" ) ) ) ;

		}

		if ( ! eventCategory.startsWith( "/csap/ui/access/" ) ) {

			// only the latest userid is stored
			Document createdOn = (Document) eventDocument.get( "createdOn" ) ;
			String date = createdOn.getString( "date" ) ;
			queryFilters.add( eq( "createdOn.date", date ) ) ;

		}

		Bson andedQueryFilters = and( queryFilters ) ;

		Document replacementEvent = new Document( ) ;
		replacementEvent.put( "$set", eventDocument ) ;
		replacementEvent.put( "$inc", new Document( ).append( "counter", 1 ) ) ;

		// if match is NOT found - insert it
		FindOneAndUpdateOptions findOptions = new FindOneAndUpdateOptions( ) ;
		findOptions.upsert( true ) ;
		findOptions.returnDocument( ReturnDocument.AFTER ) ;

		try {

			Document result = eventDataHelper
					.getMongoEventCollection( )
					.findOneAndUpdate(
							andedQueryFilters,
							replacementEvent,
							findOptions ) ;

			if ( null != result ) {

				key = result.getObjectId( "_id" ).toString( ) ;
				// if ( eventCategory.startsWith( "/csap/ui/access" ) ) {
				logger.debug( "upsert for {}, filters: {}, results:  {}", eventCategory, queryFilters,
						CsapApplication.header( result.toJson( ) ) ) ;

			}

		} catch ( Throwable e ) {

			logger.error( "Exception while inserting/updating: {}, reason: {}", eventCategory, CSAP.buildCsapStack(
					e ) ) ;
			throw e ;

		}

		var nanos = metricUtilities.stopTimer( timer, "db-event.insert.insertOrUpdate" ) ;

		logger.debug( "Time Taken {}, conditions: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				queryFilters ) ;
		return key ;

	}

	public void addPrimarySwitchedMessage ( String newPrimaryHost , String oldPrimaryHost ) {

		Document eventDocument = new Document( ) ;
		eventDocument.append( CATEGORY, "/csap/mongo/primary" ) ;
		eventDocument.append( SUMMARY, "Mongo DB primary switched" ) ;
		eventDocument.append( PROJECT, "CsapData" ) ;
		eventDocument.append( HOST, getHostName( ) ) ;
		Document dataObject = new Document( ) ;
		dataObject.append( "newPrimary", newPrimaryHost ) ;
		dataObject.append( "oldPrimary", oldPrimaryHost ) ;
		eventDocument.append( DATA, dataObject ) ;
		Document createdObDbObject = new Document( ) ;
		Calendar calendar = Calendar.getInstance( ) ;
		createdObDbObject.append( UNIXMS, calendar.getTimeInMillis( ) ) ;
		createdObDbObject.append( DATE, DateUtil.getFormatedDate( calendar ) ) ;
		createdObDbObject.append( TIME, DateUtil.getFormatedTime( calendar ) ) ;
		eventDocument.append( CREATED_ON, createdObDbObject ) ;
		// setCreationDate(dbObject);
		setMongoDate( eventDocument ) ;
		String life = System.getenv( "csapLife" ) ;
		if ( StringUtils.isBlank( life ) )
			life = "prod" ;
		eventDocument.append( LIFE, life ) ;
		eventDocument.append( APPID, "csapeng.gen" ) ;
		// setExpiresAt(dbObject);
		eventDocument.append( "expiresAt", getExpirationTime( eventDocument.getString( "lifecycle" ) ) ) ;

		try {

			// DBCollection eventCollection = getEventCollection();
			// eventCollection.insert(dbObject);
			eventDataHelper.getMongoEventCollection( )
					.insertOne( eventDocument ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while inserting primary switched event", e ) ;

		}

	}

	private String getHostName ( ) {

		try {

			InetAddress addr = InetAddress.getLocalHost( ) ;
			String hostName = addr.getHostName( ) ;
			return hostName ;

		} catch ( UnknownHostException e ) {

			logger.error( "Exception while getting host name", e ) ;

		}

		return null ;

	}

	private void addExceptionEvent ( String exceptionMessage , String eventJson , String appId ) {

		Document exceptionEventDocument = new Document( ) ;
		exceptionEventDocument.append( CATEGORY, PAYLOAD_CATEGORY ) ;
		exceptionEventDocument.append( SUMMARY, "Exception while inserting payload into mongo" ) ;
		exceptionEventDocument.append( PROJECT, "CsapData" ) ;
		exceptionEventDocument.append( HOST, "MongoDBHost" ) ;
		Document dataObject = new Document( ) ;
		dataObject.append( "exceptionMessage", exceptionMessage ) ;

		if ( StringUtils.isNotBlank( appId ) ) {

			dataObject.append( "appIdUsedForAuth", appId ) ;

		} else {

			dataObject.append( "appIdUsedForAuth", "null" ) ;

		}

		if ( StringUtils.isNotBlank( eventJson ) ) {

			dataObject.append( "eventJson", eventJson ) ;

		} else {

			dataObject.append( "eventJson", "null" ) ;

		}

		exceptionEventDocument.append( DATA, dataObject ) ;
		Document createdObDbObject = new Document( ) ;
		Calendar calendar = Calendar.getInstance( ) ;
		createdObDbObject.append( UNIXMS, calendar.getTimeInMillis( ) ) ;
		createdObDbObject.append( DATE, DateUtil.getFormatedDate( calendar ) ) ;
		createdObDbObject.append( TIME, DateUtil.getFormatedTime( calendar ) ) ;
		exceptionEventDocument.append( CREATED_ON, createdObDbObject ) ;
		setMongoDate( exceptionEventDocument ) ;

		String life = System.getenv( "csapLife" ) ;
		logger.debug( "life::{}", life ) ;
		if ( StringUtils.isBlank( life ) )
			life = "prod" ;
		exceptionEventDocument.append( LIFE, life ) ;
		exceptionEventDocument.append( APPID, "csapeng.gen" ) ;
		exceptionEventDocument.append( "expiresAt", getExpirationTime( exceptionEventDocument.getString(
				"lifecycle" ) ) ) ;

		try {

			eventDataHelper.getMongoEventCollection( )
					.insertOne( exceptionEventDocument ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while inserting exception event", e ) ;

		}

	}

	private DBCollection getEventCollection ( ) {

		return mongoClient.getDB( EVENT_DB_NAME ).getCollection( EVENT_COLLECTION_NAME ) ;

	}

	private void setMongoDate ( Document eventDocument ) {

		Document createdOn = (Document) eventDocument.get( "createdOn" ) ;

		if ( null != createdOn ) {

			Calendar calendar = Calendar.getInstance( ) ;
			createdOn.append( "lastUpdatedOn", calendar.getTime( ) ) ;
			// createdOn.append("eventCreatedOn",calendar.getTime());
			Long unixMs = createdOn.getLong( "unixMs" ) ;

			if ( null != unixMs ) {

				Date date = new Date( unixMs ) ;
				createdOn.append( "mongoDate", date ) ;
				eventDocument.put( "createdOn", createdOn ) ;

			}

		} else {

			createdOn = new Document( ) ;
			Calendar calendar = Calendar.getInstance( ) ;
			createdOn.append( "lastUpdatedOn", calendar.getTime( ) ) ;
			createdOn.append( "eventCreatedOn", calendar.getTime( ) ) ;
			createdOn.append( "mongoDate", calendar.getTime( ) ) ;
			createdOn.append( "date", DateUtil.getFormatedDate( calendar ) ) ;
			createdOn.append( "time", DateUtil.getFormatedTime( calendar ) ) ;
			createdOn.append( "unixMs", calendar.getTimeInMillis( ) ) ;
			eventDocument.put( "createdOn", createdOn ) ;

		}

	}

}
