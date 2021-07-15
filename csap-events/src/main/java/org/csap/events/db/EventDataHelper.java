package org.csap.events.db ;

import static com.mongodb.client.model.Filters.and ;
import static com.mongodb.client.model.Filters.eq ;
import static com.mongodb.client.model.Filters.gte ;
import static com.mongodb.client.model.Filters.in ;
import static com.mongodb.client.model.Filters.lt ;
import static com.mongodb.client.model.Filters.regex ;
import static com.mongodb.client.model.Projections.excludeId ;
import static com.mongodb.client.model.Projections.fields ;
import static com.mongodb.client.model.Projections.include ;
import static com.mongodb.client.model.Sorts.descending ;
import static org.csap.events.EventJsonConstants.CATEGORY ;
import static org.csap.events.EventJsonConstants.CREATED_ON ;
import static org.csap.events.EventJsonConstants.DATE ;
import static org.csap.events.EventJsonConstants.HOST ;
import static org.csap.events.util.MongoConstants.EVENT_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.EVENT_DB_NAME ;

import java.util.ArrayList ;
import java.util.Calendar ;
import java.util.Date ;
import java.util.List ;
import java.util.regex.Pattern ;
import java.util.stream.Collectors ;
import java.util.stream.StreamSupport ;

import javax.inject.Inject ;

import org.apache.commons.lang3.StringUtils ;
import org.bson.Document ;
import org.bson.conversions.Bson ;
import org.bson.types.ObjectId ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.EventJsonConstants ;
import org.csap.events.util.DateUtil ;
import org.csap.helpers.CsapApplication ;
import org.csap.helpers.CsapRestTemplateFactory ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.cache.annotation.Cacheable ;
import org.springframework.stereotype.Service ;

import com.mongodb.BasicDBList ;
import com.mongodb.BasicDBObject ;
import com.mongodb.DBCollection ;
import com.mongodb.DBObject ;
import com.mongodb.MongoClient ;
import com.mongodb.MongoClientOptions ;
import com.mongodb.MongoCredential ;
import com.mongodb.ReadPreference ;
import com.mongodb.ServerAddress ;
import com.mongodb.client.FindIterable ;
import com.mongodb.client.MongoCollection ;
import com.mongodb.client.result.DeleteResult ;

@Service
public class EventDataHelper {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Inject
	private MongoClient mongoClient ;

	@Deprecated
	@Cacheable ( "healthReportCache" )
	public boolean isHealthReportEnabled ( String projectName , String life ) {

		DBCollection dbCollection = mongoClient.getDB( EVENT_DB_NAME ).getCollection( "displayNameCollection" ) ;
		BasicDBObject query = new BasicDBObject( "_id", projectName ) ;
		DBObject configuration = dbCollection.findOne( query ) ;

		if ( null != configuration ) {

			BasicDBList healthEnabledList = (BasicDBList) configuration.get( "health" ) ;

			if ( null != healthEnabledList ) {

				if ( healthEnabledList.contains( life ) ) {

					return true ;

				}

			}

		}

		return false ;

	}

	boolean showNoHealthSettings = true ;

	@Cacheable ( "healthReportCache" )
	public boolean isSaveHealthReport ( String projectName , String life ) {

		boolean isHealthForProjectEnabled = false ;

		MongoCollection<Document> eventCollection = getMongoEventCollection( ) ;

		Document analyticsSettings = eventCollection.find(
				eq( EventJsonConstants.CATEGORY, "/csap/settings/analytics" ) )
				.sort( descending( "createdOn.lastUpdatedOn" ) )
				.projection( fields( include( "data" ), excludeId( ) ) )
				.limit( 1 )
				.first( ) ;

		if ( analyticsSettings == null ) {

			if ( showNoHealthSettings ) {

				logger.warn(
						"No settings found: '/csap/settings/analytics'. All host events will be stored (use CSAP data viewer to throttle if needed)." ) ;
				showNoHealthSettings = false ;

			}

			isHealthForProjectEnabled = true ;

		} else if ( analyticsSettings.get( "data" ) instanceof List ) {

			logger.debug( "Settings ::{}", analyticsSettings ) ;
			List<Document> data = (List<Document>) analyticsSettings.get( "data" ) ;

			for ( Document document : data ) {

				if ( projectName.equalsIgnoreCase( document.getString( "project" ) ) ) {

					if ( document.get( "health" ) instanceof List ) {

						List healthEnabledLifes = (List) document.get( "health" ) ;

						if ( healthEnabledLifes.contains( life ) ) {

							isHealthForProjectEnabled = true ;

						}

					}

				}

			}

		}

		return isHealthForProjectEnabled ;

	}

	public Document constructCategoryHostDateDocumentQuery ( Document eventDocument ) {

		Document query = new Document( ) ;
		query.append( CATEGORY, eventDocument.get( CATEGORY ) ) ;
		query.append( HOST, eventDocument.get( HOST ) ) ;
		Document createdOn = (Document) eventDocument.get( CREATED_ON ) ;
		String date = createdOn.getString( DATE ) ;
		query.append( CREATED_ON + "." + DATE, date ) ;
		return query ;

	}

	@Deprecated
	public BasicDBObject constructCategoryHostDateQuery ( BasicDBObject dbObject ) {

		BasicDBObject query = new BasicDBObject( ) ;
		query.append( CATEGORY, dbObject.get( CATEGORY ) ) ;
		query.append( HOST, dbObject.get( HOST ) ) ;
		BasicDBObject createdOn = (BasicDBObject) dbObject.get( CREATED_ON ) ;
		String date = createdOn.getString( DATE ) ;
		query.append( CREATED_ON + "." + DATE, date ) ;
		return query ;

	}

	public Bson buildAppIdAndLifeFilter ( String userRequest ) {

		List<Bson> searchIndexedCriteria = new ArrayList<>( ) ;

		String[] searchParameters = userRequest.split( "," ) ;

		for ( String searchParameter : searchParameters ) {

			if ( searchParameter.contains( "eventReceivedOn" ) || searchParameter.contains( "isDataRequired" ) ) {

				continue ;

			}

			String[] filterNameAndValue = searchParameter.split( "=" ) ;

			if ( filterNameAndValue.length != 2 ) {

				continue ;

			}

			String fieldName = filterNameAndValue[0] ;
			String fieldValue = filterNameAndValue[1] ;

			if ( EventJsonConstants.APPID.equalsIgnoreCase( fieldName ) ) {

				searchIndexedCriteria.add( eq( EventJsonConstants.APPID, fieldValue ) ) ;

			}

			if ( EventJsonConstants.LIFE.equalsIgnoreCase( fieldName ) ) {

				searchIndexedCriteria.add( eq( EventJsonConstants.LIFE, fieldValue ) ) ;

			}

		}

		Bson filter = new Document( ) ;

		if ( searchIndexedCriteria.size( ) > 0 ) {

			filter = and( searchIndexedCriteria ) ;

		}

		return filter ;

	}

	public Bson convertUserInterfaceQueryToMongoFilter ( String userRequest ) {

		List<Bson> searchIndexedCriteria = new ArrayList<>( ) ;

		// List<Bson> searchNotIndexCriteria = new ArrayList<>();
		if ( null == userRequest ) {

			return new Document( ) ; // empty filter

		}

		Bson categoryPattern = null ; // must be at the end or index is bypassed.

		String[] searchParameters = userRequest.split( "," ) ;
		//
		Date fromDate = null ;
		Date toDate = null ;

		for ( String searchParameter : searchParameters ) {

			if ( searchParameter.contains( "eventReceivedOn" ) || searchParameter.contains( "isDataRequired" ) ) {

				continue ;

			}

			String[] filterNameAndValue = searchParameter.split( "=" ) ;

			if ( filterNameAndValue.length != 2 ) {

				continue ;

			}

			String fieldName = filterNameAndValue[0] ;
			String fieldValue = filterNameAndValue[1] ;
			// simpleSearchText is the main ui field. Datatables api depends on
			// this field name.
			// Category is entered in this field

			if ( "simpleSearchText".equalsIgnoreCase( fieldName ) ) {

				String categorySearchText = fieldValue ;
				boolean useRegEx = isUseRegEx( fieldValue ) ;

				if ( fieldValue.endsWith( "*" ) ) {

					categorySearchText = fieldValue.substring( 0, fieldValue.indexOf( "*" ) ) ;

				}

				if ( useRegEx ) {

					// Pattern pattern = Pattern.compile( "^" +
					// categorySearchText, Pattern.CASE_INSENSITIVE );
					Pattern pattern = Pattern.compile( "^" + categorySearchText ) ;
					searchIndexedCriteria.add( regex( EventJsonConstants.CATEGORY, pattern ) ) ;

				} else {

					searchIndexedCriteria.add( eq( EventJsonConstants.CATEGORY, categorySearchText ) ) ;

				}
				// conds.add( regex( EventJsonConstants.CATEGORY, "^/csap/ui/" )
				// );

			} else if ( EventJsonConstants.SUMMARY.equalsIgnoreCase( fieldName ) ) {

				Pattern pattern = Pattern.compile( fieldValue, Pattern.CASE_INSENSITIVE ) ;
				searchIndexedCriteria.add( eq( EventJsonConstants.SUMMARY, pattern ) ) ;

			} else if ( "from".equalsIgnoreCase( fieldName ) ) {

				fromDate = DateUtil.convertUserDateToJavaDate( fieldValue ) ;

			} else if ( "to".equalsIgnoreCase( fieldName ) ) {

				toDate = DateUtil.convertUserDateToJavaDate( fieldValue ) ;
				Calendar calendar = Calendar.getInstance( ) ;
				calendar.setTime( toDate ) ;
				calendar.add( Calendar.DATE, 1 ) ;
				toDate = calendar.getTime( ) ;

			} else {

				searchIndexedCriteria.add( eq( fieldName, fieldValue ) ) ;

			}

		}

		Bson dateRange = createDateRangeCondition( fromDate, toDate ) ;

		if ( null != dateRange ) {

			searchIndexedCriteria.add( dateRange ) ;

		}

		Bson filter = new Document( ) ;

		if ( searchIndexedCriteria.size( ) > 0 ) {

			filter = and( searchIndexedCriteria ) ;

		}

		logger.debug( "UI search string: {}\n resolved to: \n {}", userRequest, filter ) ;
		return filter ;

	}

	// clean up csagent updated on all labs
	public boolean isUseRegEx ( String category ) {

		if ( category.endsWith( "*" )
				|| category.equalsIgnoreCase( "/csap/ui" )
				|| category.equalsIgnoreCase( "/csap/ui/svc" )
				|| category.equalsIgnoreCase( "/csap" ) ) {

			return true ;

		}

		return false ;

	}

	private Bson createDateRangeCondition ( Date fromDate , Date toDate ) {

		Bson dateRange = null ;

		if ( null == fromDate && null != toDate ) {

			fromDate = toDate ;

		}

		if ( null == toDate && null != fromDate ) {

			toDate = fromDate ;

		}

		if ( null != fromDate && null != toDate ) {

			if ( fromDate.equals( toDate ) ) {

				Calendar calendar = Calendar.getInstance( ) ;
				calendar.setTime( toDate ) ;
				calendar.add( Calendar.DATE, 1 ) ;
				toDate = calendar.getTime( ) ;

			}

			dateRange = and( gte( "createdOn.date", DateUtil.convertJavaDateToMongoCreatedDate( fromDate ) ),
					lt( "createdOn.date", DateUtil.convertJavaDateToMongoCreatedDate( toDate ) ) ) ;

		}

		return dateRange ;

	}

	public void keepMostRecentAndDeleteOthers ( String category , String appId , String life , int keepMostRecent ) {

		if ( keepMostRecent > 0 ) {

			MongoCollection<Document> eventCollection = getMongoEventCollection( ) ;
			Bson filter = constructBsonQuery( appId, life, null, null, category ) ;
			FindIterable<Document> docsToBeDeleted = eventCollection.find( filter )
					.sort( descending( "createdOn.lastUpdatedOn" ) )
					.skip( keepMostRecent )
					.projection( include( "_id" ) ) ;

			List<ObjectId> deleteList = StreamSupport.stream( docsToBeDeleted.spliterator( ), false )
					.map( idObj -> (ObjectId) idObj.get( "_id" ) )
					.collect( Collectors.toList( ) ) ;
			logger.debug( "Delete list {}", deleteList ) ;

			if ( deleteList.size( ) > 0 ) {

				DeleteResult deleteResult = eventCollection.deleteMany( in( "_id", deleteList ) ) ;
				logger.info( "Deleted {} records", deleteResult.getDeletedCount( ) ) ;

			}

		}

	}

	public Bson constructBsonQuery ( String appId , String life , String project , String category , String host ) {

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

			conds.add( eq( EventJsonConstants.CATEGORY, category ) ) ;

		}

		if ( StringUtils.isNotBlank( host ) ) {

			conds.add( eq( EventJsonConstants.HOST, host ) ) ;

		}

		Bson filter = new Document( ) ;

		if ( conds.size( ) > 0 ) {

			filter = and( conds ) ;

		}

		return filter ;

	}

	public Document constructDocumentQuery ( String appId , String life , String project , String category ) {

		Document query = new Document( ) ;

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

			query.append( EventJsonConstants.CATEGORY, category ) ;

		}

		return query ;

	}

	public void addDateToQuery ( Document query , String fromDate , String toDate ) {

		if ( StringUtils.isBlank( fromDate ) || StringUtils.isBlank( toDate ) ) {

			String date = DateUtil.getFormatedDate( Calendar.getInstance( ) ) ;
			query.append( "createdOn.date", date ) ;

		} else {

			String formatedFromDate = DateUtil.convertUserDateToMongoCreatedDate( fromDate ) ;
			Date tDate = DateUtil.convertUserDateToJavaDate( toDate ) ;
			Calendar calendar = Calendar.getInstance( ) ;
			calendar.setTime( tDate ) ;
			calendar.add( Calendar.DATE, 1 ) ;
			String formatedToDate = DateUtil.convertJavaDateToMongoCreatedDate( calendar.getTime( ) ) ;
			Document dateRange = new Document( "$gte", formatedFromDate ).append( "$lte", formatedToDate ) ;
			query.append( "createdOn.date", dateRange ) ;

		}

	}

	// public
	@Deprecated
	public DBCollection getEventCollection ( ) {

		return mongoClient.getDB( EVENT_DB_NAME ).getCollection( EVENT_COLLECTION_NAME ) ;

	}

	@Inject
	CsapEventsApplication csapEventsApplication ;

	public Document serverStatus ( )
		throws Exception {

		var timer = metricUtilities.startTimer( ) ;

		List<MongoCredential> credentialList = new ArrayList<>( ) ;
		credentialList.add( csapEventsApplication.getMongoCredential( ) ) ;

		String currentHost = "localhost" ;

		if ( ! CsapApplication.isCsapFolderSet( ) ) {

			currentHost = csapEventsApplication.getMongoConfig( ).getHosts( ).split( " " )[0] ;

		}

		Document serverJson = null ;
		MongoClientOptions options = MongoClientOptions
				.builder( )
				.connectTimeout( 1000 )
				.serverSelectionTimeout( 100 )
				.build( ) ;

		try ( MongoClient directConnection = new MongoClient(
				new ServerAddress( currentHost, csapEventsApplication.getMongoConfig( ).getPort( ) ),
				credentialList, options ) ) {

			serverJson = directConnection
					.getDatabase( "admin" )
					.withReadPreference( ReadPreference.nearest( ) )
					.runCommand( new Document( "serverStatus", 1 ) ) ;

		} catch ( Exception e ) {

			logger.error( "Failed Server status {}",
					CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) ) ;
			throw new Exception( "Failed to connect to mongo" ) ;

		}

		metricUtilities.stopTimer( timer, "csap.mongo-server-status" ) ;

		return serverJson ;

	}

	public MongoCollection<Document> getMongoEventCollection ( ) {

		// return
		// mongoClient.getDatabase("event").getCollection("eventRecords").withReadPreference(ReadPreference.secondaryPreferred());
		return mongoClient.getDatabase( "event" ).getCollection( "eventRecords" )
				.withReadPreference( ReadPreference.secondaryPreferred( ) ) ;

	}

	public MongoCollection<Document> getMetricsCollectionByCategory ( String category ) {

		MongoCollection<Document> dbCollection = null ;

		if ( category.startsWith( "/csap/metrics" ) && category.endsWith( "data" ) ) {

			dbCollection = mongoClient.getDatabase( "metricsDb" ).getCollection( "metrics" )
					.withReadPreference( ReadPreference.secondaryPreferred( ) ) ;

		} else if ( category.startsWith( "/csap/metrics" ) && category.endsWith( "attributes" ) ) {

			dbCollection = mongoClient.getDatabase( "metricsDb" ).getCollection( "metricsAttributes" )
					.withReadPreference( ReadPreference.secondaryPreferred( ) ) ;

		}

		return dbCollection ;

	}

}
