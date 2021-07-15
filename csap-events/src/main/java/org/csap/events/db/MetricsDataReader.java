package org.csap.events.db ;

import static com.mongodb.client.model.Projections.exclude ;
import static com.mongodb.client.model.Projections.excludeId ;
import static com.mongodb.client.model.Projections.fields ;
import static com.mongodb.client.model.Projections.include ;
import static org.csap.events.util.MetricsJsonConstants.ATTRIBUTES ;
import static org.csap.events.util.MetricsJsonConstants.CREATED_ON ;
import static org.csap.events.util.MetricsJsonConstants.DATA ;
import static org.csap.events.util.MetricsJsonConstants.HOST_NAME ;
import static org.csap.events.util.MetricsJsonConstants.ID ;
import static org.csap.events.util.MetricsJsonConstants.LAST_UPDATED_ON ;
import static org.csap.events.util.MongoConstants.EVENT_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.EVENT_DB_NAME ;
import static org.csap.events.util.MongoConstants.METRICS_ATTRIBUTES_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.METRICS_DATA_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.METRICS_DB_NAME ;

import java.io.IOException ;
import java.util.ArrayList ;
import java.util.Calendar ;
import java.util.Date ;
import java.util.HashMap ;
import java.util.List ;
import java.util.Map ;
import java.util.Set ;
import java.util.concurrent.TimeUnit ;

import javax.inject.Inject ;

import org.apache.commons.lang3.StringUtils ;
import org.bson.Document ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.util.EventJsonConstants ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.cache.annotation.Cacheable ;

import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ArrayNode ;
import com.fasterxml.jackson.databind.node.JsonNodeFactory ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.MongoClient ;
import com.mongodb.ReadPreference ;
import com.mongodb.client.FindIterable ;
import com.mongodb.client.MongoCollection ;
import com.mongodb.client.MongoCursor ;

public class MetricsDataReader {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Inject
	private MongoClient mongoClient ;

	@Inject
	private ObjectMapper jsonMapper ;

	@Cacheable ( value = CsapEventsApplication.NUM_DAYS_CACHE )
	public long retrieveNumDaysOfMetrics ( String hostName ) {

		var timer = metricUtilities.startTimer( ) ;
		long numDaysAvailable = 0 ;
		Document query = new Document( "attributes.hostName", hostName ) ;
		Document sortOrder = new Document( "createdOn.date", 1 ) ;
		Document metricsDataObject = getMetricsMongoCollection( ).find( query )
				.sort( sortOrder )
				.limit( 1 )
				.projection( fields( include( "createdOn" ), excludeId( ) ) )
				.first( ) ;

		if ( null != metricsDataObject ) {

			Document createdOnDbObject = (Document) metricsDataObject.get( CREATED_ON ) ;
			Date earliestDate = (Date) createdOnDbObject.get( "mongoDate" ) ;
			logger.debug( "Earliest Date ::--> {}", earliestDate ) ;
			long diff = Calendar.getInstance( ).getTimeInMillis( ) - earliestDate.getTime( ) ;
			numDaysAvailable = TimeUnit.DAYS.convert( diff, TimeUnit.MILLISECONDS ) ;

		}

		metricUtilities.stopTimer( timer, "numDaysOfMetrics" ) ;
		return numDaysAvailable ;

	}

	@Cacheable ( CsapEventsApplication.ATTRIBUTES_CACHE )
	public Document getMetricsAttribute (
											String hostName ,
											String requestedMetricsCollection ,
											int numberOfDaysToRetreive ,
											int numDaysOffsetFromToday ,
											boolean showDaysFrom ) {

		Document attributeOject = null ;
		Document query = constructAttributeQueryUsingLatestAttributesOnDayTarget(
				hostName, requestedMetricsCollection, numberOfDaysToRetreive, numDaysOffsetFromToday, showDaysFrom ) ;

		Document sortOrder = new Document( ) ;
		sortOrder.append( CREATED_ON + "." + LAST_UPDATED_ON, -1 ) ;

		//
		// Gets the attributes on the first day: what if service was removed, and in
		// oldest day?
		//
		FindIterable<Document> findResult = getMetricsAttributeMongoCollection( ).find( query )
				.sort( sortOrder )
				.limit( 1 )
				.projection( fields( exclude( "createdOn", "expiresAt" ), excludeId( ) ) ) ;

		MongoCursor<Document> attributeCursor = findResult.iterator( ) ;

		if ( attributeCursor.hasNext( ) ) {

			while ( attributeCursor.hasNext( ) ) {

				attributeOject = (Document) attributeCursor.next( ) ;

			}

		} else {

			logger.warn( "requestedMetricsCollection {}:  Did not find attributes in date range. Using latest",
					requestedMetricsCollection ) ;
			query = constructAttributeQuery( hostName, requestedMetricsCollection ) ;
			// cursor = dbCollection.find(query,keys).sort(sortOrder);
			findResult = getMetricsAttributeMongoCollection( ).find( query )
					.sort( sortOrder )
					.projection( fields( exclude( "createdOn", "expiresAt" ), excludeId( ) ) ) ;
			attributeCursor = findResult.iterator( ) ;

			while ( attributeCursor.hasNext( ) ) {

				logger.debug( "Found in latest" ) ;
				attributeOject = (Document) attributeCursor.next( ) ;

			}

		}

		return attributeOject ;

	}

	// Significant objects stored in memory DO NOT CACHE PARSED OBJECTS
	public ObjectNode findAndMergeMetricData (
												String hostName ,
												String metricId ,
												int numberOfDaysToRetreive ,
												int numDaysOffsetFromToday ,
												String appId ,
												String life ,
												boolean showDaysFrom ,
												boolean padLatest ,
												List<String> metricAttributeNames ) {

		var mergedGraphReport = jsonMapper.createObjectNode( ) ;

		var mongoDocumentCursor = getMetricsData( hostName, metricId, numberOfDaysToRetreive, numDaysOffsetFromToday,
				showDaysFrom ) ;

		int capacity = getListCapacity( metricId, numberOfDaysToRetreive ) ;
		Date latestReportDate = null ;

		logger.debug( "{}:{} results filter: {}", hostName, metricId, metricAttributeNames ) ;

		var numOfAttributesPadded = 0 ;
		var misMatchAttributeName = "" ;

		while ( mongoDocumentCursor.hasNext( ) ) {

			Document dbObject = mongoDocumentCursor.next( ) ;

			if ( null == latestReportDate ) {

				Document createdOn = (Document) dbObject.get( "createdOn" ) ;
				latestReportDate = (Date) createdOn.get( "mongoDate" ) ;

			}

			Document dataObject = (Document) dbObject.get( DATA ) ;
			var collectedAttributeValues = new HashMap<String, ArrayNode>( ) ;

			for ( var attributeName : metricAttributeNames ) {

				List<?> attributeData = (List<?>) dataObject.get( attributeName ) ;

				logger.debug( "attribute: {} , atttributeDataObject: {}", attributeName, attributeData ) ;

				if ( null != attributeData ) {

					if ( attributeName.equals( "timeStamp" ) || attributeName.contains( "threadCount" )
							|| attributeName.contains( "Cpu" ) ) {

						// logger.info( "Merging {}: {}", attributeName, attributeData.size() ) ;
					}

					try {

						var attributesValuesAsJson = (ArrayNode) jsonMapper.readTree( attributeData.toString( ) ) ;
						collectedAttributeValues.put( attributeName, attributesValuesAsJson ) ;

					} catch ( IOException e ) {

						logger.warn( "Failed parsing mongo data into json: " ) ;

					}

				}

			}

			var timeStampCount = collectedAttributeValues.get( "timeStamp" ).size( ) ;

			for ( var attributeName : metricAttributeNames ) {

				var collectedValues = collectedAttributeValues.get( attributeName ) ;

				if ( collectedValues == null ) {

					collectedValues = jsonMapper.createArrayNode( ) ;

				}

				if ( timeStampCount != collectedValues.size( ) ) {

					numOfAttributesPadded++ ;
					misMatchAttributeName = attributeName ;

					while ( collectedValues.size( ) < timeStampCount ) {

						collectedValues.add( 0 ) ;

					}

				}

				var mergedData = mergedGraphReport.path( attributeName ) ;

				if ( mergedData.isMissingNode( ) ) {

					mergedData = JsonNodeFactory.instance.arrayNode( capacity ) ;
					mergedGraphReport.set( attributeName, mergedData ) ;

				}

				( (ArrayNode) mergedData ).addAll( collectedValues ) ;

			}

		}

		mongoDocumentCursor.close( ) ;

		if ( numOfAttributesPadded > 0 ) {

			logger.warn( "MisMatch data: {} :  {} , number of documents padded: {}, last attribute Mismatch: {} ",
					hostName, metricId, numOfAttributesPadded, misMatchAttributeName ) ;

		}
		// logger.info( "graphData: {}", graphData );

		if ( padLatest ) {

			if ( numDaysOffsetFromToday == 0 && StringUtils.isNotEmpty( life ) ) {

				padLatestWithShortestCollectionInterval( appId, life, metricId, hostName, latestReportDate,
						metricAttributeNames,
						capacity,
						mergedGraphReport ) ;

			} else {

				logger.warn( "Padding is being skipped because lifecycle was empty :: {}", life ) ;

			}

		}

		return mergedGraphReport ;

	}

	// For performance - only 30 second data is uploaded every 30 minutes
	// so we append it to other collections
	private void padLatestWithShortestCollectionInterval (
															String appId ,
															String life ,
															String requestedMetricId ,
															String hostName ,
															Date latestReportDate ,
															List<String> metricAttributeNames ,
															int capacity ,
															ObjectNode mergedGraphReport ) {

		Map<String, List> metricToIntervalsAvailable = getMetricsInterval( appId, life ) ;
		String smallestMetricIntervalId = getSmallestIntervalId( metricToIntervalsAvailable, requestedMetricId ) ;
		logger.debug( "requestedMetricId: {}, smallestMetricIntervalId: {}, metricToIntervalsAvailable: {}",
				requestedMetricId, smallestMetricIntervalId, metricToIntervalsAvailable ) ;

		if ( isIntervalSmallest( metricToIntervalsAvailable, requestedMetricId ) ) {

			// no need to aggregate - selected interval already has the latest data
			return ;

		}

		// now get lowest interval report from latest report date
		MongoCursor<Document> currentDaycursor = getMetricsData( hostName, smallestMetricIntervalId,
				latestReportDate ) ;
		Map<String, List> currentDayGraphData = new HashMap<>( ) ;

		while ( currentDaycursor.hasNext( ) ) {

			Document dbObject = currentDaycursor.next( ) ;
			Document createdOn = (Document) dbObject.get( "createdOn" ) ;
			Date reportDate = (Date) createdOn.get( "mongoDate" ) ;
			Document dataObject = (Document) dbObject.get( DATA ) ;

			// logger.info( "Searching {} ", dbObject.toJson() ) ;

			for ( var attributeName : metricAttributeNames ) {

				List atttributeDataObject = (List) dataObject.get( attributeName ) ;

				if ( null != atttributeDataObject ) {

					if ( null != currentDayGraphData.get( attributeName ) ) {

						currentDayGraphData.get( attributeName ).addAll( atttributeDataObject ) ;

					} else {

						ArrayList dataList = new ArrayList( ) ;
						dataList.ensureCapacity( capacity ) ;
						dataList.addAll( atttributeDataObject ) ;
						currentDayGraphData.put( attributeName, dataList ) ;

					}

				}

			}

		}

		if ( ! currentDayGraphData.isEmpty( ) ) {

			var numOfAttributesPadded = 0 ;
			var misMatchAttributeName = "" ;

			logger.debug( "Updating graphs with: {}", currentDayGraphData.keySet( ) ) ;

			var latestData = jsonMapper.createObjectNode( ) ;

			for ( String key : currentDayGraphData.keySet( ) ) {

				try {

					var attributesValuesAsJson = (ArrayNode) jsonMapper.readTree( currentDayGraphData.get( key )
							.toString( ) ) ;
					latestData.set( key, attributesValuesAsJson ) ;

				} catch ( IOException e ) {

					logger.warn( "Failed parsing mongo data into json: " ) ;

				}

			}

			var timeStampCount = latestData.path( "timeStamp" ).size( ) ;
			logger.debug( "Adding {} new values", timeStampCount ) ;

			for ( var attributeName : metricAttributeNames ) {

				var collectedValues = latestData.path( attributeName ) ;

				if ( ! collectedValues.isArray( ) ) {

					// requested attribute is NOT in 30 second collection
					collectedValues = jsonMapper.createArrayNode( ) ;

				}

				if ( timeStampCount != collectedValues.size( ) ) {

					numOfAttributesPadded++ ;
					misMatchAttributeName = attributeName ;

					while ( collectedValues.size( ) < timeStampCount ) {

						( (ArrayNode) collectedValues ).add( 0 ) ;

					}

				}

				var mergedAttributeValues = mergedGraphReport.path( attributeName ) ;

				if ( mergedAttributeValues.isArray( ) ) {

					var combinedData = JsonNodeFactory.instance.arrayNode( capacity + collectedValues.size( ) ) ;
					combinedData.addAll( (ArrayNode) collectedValues ) ;
					combinedData.addAll( (ArrayNode) mergedAttributeValues ) ;
					mergedGraphReport.set( attributeName, combinedData ) ;

				}

			}

			if ( numOfAttributesPadded > 0 ) {

				logger.warn( "MisMatch data: {} :  {} , number of documents padded: {}, last attribute Mismatch: {} ",
						hostName, requestedMetricId, numOfAttributesPadded, misMatchAttributeName ) ;

			}

		}

	}

	private String getSmallestIntervalId ( Map<String, List> metricsInterval , String id ) {

		String[] idArray = id.split( "_" ) ;

		if ( null != idArray && idArray.length > 1 ) {

			String idString = idArray[0] ;

			if ( id.startsWith( "jmx" ) ) {

				idString = "jmx" ;

			}

			List intervals = metricsInterval.get( idString ) ;

			if ( null != intervals && intervals.size( ) > 0 ) {

				return idArray[0] + "_" + intervals.get( 0 ) ;

			}

		}

		return id ;

	}

	private boolean isIntervalSmallest ( Map<String, List> metricsInterval , String id ) {

		String[] idArray = id.split( "_" ) ;

		if ( null != idArray && idArray.length > 1 ) {

			String idString = idArray[0] ;

			if ( id.startsWith( "jmx" ) ) {

				idString = "jmx" ;

			}

			List intervals = metricsInterval.get( idString ) ;

			if ( null != intervals && intervals.size( ) > 0 ) {

				if ( ! idArray[1].equalsIgnoreCase( "" + intervals.get( 0 ) ) ) {

					return false ;

				}

			}

		}

		return true ;

	}

	private Map<String, List> getMetricsInterval ( String appId , String life ) {

		Document query = new Document( ) ;
		query
				.append( "appId", appId )
				.append( "lifecycle", life )
				.append( "category", EventJsonConstants.CSAP_MODEL_SUMMAY_CATEGORY ) ;

		Document sortOrder = new Document( ) ;
		sortOrder.append( CREATED_ON + "." + LAST_UPDATED_ON, -1 ) ;
		Document dbObj = getMongoEventCollection( ).find( query )
				.sort( sortOrder )
				.limit( 1 )
				.projection( fields( include( "data.packages" ), excludeId( ) ) )
				.first( ) ;

		Map<String, List> metricsIntervals = new HashMap<>( ) ;

		if ( null != dbObj ) {

			Document dataObj = (Document) dbObj.get( "data" ) ;
			List packages = (List) dataObj.get( "packages" ) ;

			for ( Object pack : packages ) {

				Document dbPackObj = (Document) pack ;
				Document metricsObj = (Document) dbPackObj.get( "metrics" ) ;

				if ( null != metricsObj ) {

					Set<String> keySet = metricsObj.keySet( ) ;

					for ( String key : keySet ) {

						List metricsArray = (List) metricsObj.get( key ) ;
						metricsIntervals.put( key, metricsArray ) ;

					}

				}

			}

		}

		logger.debug( "Metrics intervals {} ", metricsIntervals ) ;
		return metricsIntervals ;

	}

	private int getListCapacity ( String id , int numberOfDays ) {

		int capacity = 300 ;
		String[] idArr = id.split( "_" ) ;

		if ( id.length( ) > 1 ) {

			capacity = ( ( 24 * 60 * 60 ) * numberOfDays ) / ( Integer.parseInt( idArr[1] ) ) ;

		}

		capacity++ ;
		return capacity ;

	}

	private MongoCursor<Document> getMetricsData ( String hostName , String id , Date startDate ) {

		Document query = new Document( ) ;
		query.append( ATTRIBUTES + "." + HOST_NAME, hostName ) ;
		query.append( ATTRIBUTES + "." + ID, id ) ;

		if ( null == startDate ) {

			startDate = Calendar.getInstance( ).getTime( ) ;

		}

		long startTime = startDate.getTime( ) + ( 10 * 60 * 1000 ) ;
		startDate.setTime( startTime ) ;
		query.append( CREATED_ON + "." + LAST_UPDATED_ON, new Document( "$gt", startDate ) ) ;
		Document sortOrder = new Document( ) ;
		sortOrder.append( CREATED_ON + "." + LAST_UPDATED_ON, -1 ) ;
		FindIterable<Document> findResult = getMetricsMongoCollection( ).find( query )
				.sort( sortOrder )
				.projection( fields( include( "data", "createdOn" ), excludeId( ) ) ) ;
		return findResult.iterator( ) ;

	}

	public MongoCursor<Document> getMetricsData (
													String hostName ,
													String id ,
													int numberOfDays ,
													int numDaysOffsetFromToday ,
													boolean showDaysFrom ) {

		Document query = constructQuery( hostName, id, numberOfDays, numDaysOffsetFromToday, showDaysFrom ) ;

		logger.debug( "loading metrics data: {} ", query ) ;
		Document sortOrder = new Document( ) ;
		sortOrder.append( CREATED_ON + "." + LAST_UPDATED_ON, -1 ) ;
		FindIterable<Document> findResult = getMetricsMongoCollection( ).find( query )
				.sort( sortOrder )
				.projection( fields( include( "data", "createdOn" ), excludeId( ) ) ) ;
		return findResult.iterator( ) ;

	}

	private Document constructQuery (
										String hostName ,
										String id ,
										int numDays ,
										int numDaysOffsetFromToday ,
										boolean showDaysFrom ) {

		Document query = new Document( ) ;
		query.append( ATTRIBUTES + "." + HOST_NAME, hostName ) ;
		query.append( ATTRIBUTES + "." + ID, id ) ;

		Calendar startTime = Calendar.getInstance( ) ;
		Calendar endTime = Calendar.getInstance( ) ;

		if ( numDaysOffsetFromToday <= 0 ) {

			int fromDateOffSet = numDays + numDaysOffsetFromToday ;
			int toDateOffSet = numDaysOffsetFromToday ;
			startTime.add( Calendar.DAY_OF_YEAR, -( fromDateOffSet ) ) ;
			endTime.add( Calendar.DAY_OF_YEAR, -( toDateOffSet ) ) ;

		} else {

			int fromDateOffSet = 0 ;
			int toDateOffSet = 0 ;

			if ( showDaysFrom ) {

				fromDateOffSet = numDaysOffsetFromToday ;
				toDateOffSet = numDaysOffsetFromToday - numDays ;

			} else {

				fromDateOffSet = ( numDaysOffsetFromToday + numDays ) - 1 ;
				toDateOffSet = numDaysOffsetFromToday - 1 ;

			}

			startTime.add( Calendar.DAY_OF_YEAR, -( fromDateOffSet ) ) ;
			endTime.add( Calendar.DAY_OF_YEAR, -( toDateOffSet ) ) ;

			endTime.set( Calendar.HOUR_OF_DAY, 0 ) ;
			endTime.set( Calendar.MINUTE, 0 ) ;
			endTime.set( Calendar.SECOND, 0 ) ;
			endTime.set( Calendar.MILLISECOND, 0 ) ;

			startTime.set( Calendar.HOUR_OF_DAY, 0 ) ;
			startTime.set( Calendar.MINUTE, 0 ) ;
			startTime.set( Calendar.SECOND, 0 ) ;
			startTime.set( Calendar.MILLISECOND, 0 ) ;

		}

		query.append( CREATED_ON + "." + LAST_UPDATED_ON,
				new Document( "$gte", startTime.getTime( ) ).append( "$lte", endTime.getTime( ) ) ) ;
		// logger.debug("Query::"+query);
		return query ;

	}

	private Document constructAttributeQuery ( String hostName , String id ) {

		Document query = new Document( ) ;
		query.append( HOST_NAME, hostName ) ;
		query.append( ID, id ) ;
		// Calendar calendar = Calendar.getInstance();
		// calendar.add(Calendar.DAY_OF_YEAR, (-numDays));
		// query.append(CREATED_ON+"."+LAST_UPDATED_ON, new
		// BasicDBObject("$gte",calendar.getTime()));
		// logger.debug("Query::"+query);
		return query ;

	}

	//
	// Attributes are only uploaded if a host restarts or they have changed
	// --- get the LAST attributes uploader, prior to the interval requested by
	// numDaysOffsetFromToday
	private Document constructAttributeQueryUsingLatestAttributesOnDayTarget (
																				String hostName ,
																				String id ,
																				int numberOfDaysToRetreive ,
																				int numDaysOffsetFromToday ,
																				boolean showDaysFrom ) {

		Document query = new Document( ) ;
		query.append( HOST_NAME, hostName ) ;
		query.append( ID, id ) ;
		Calendar startTime = Calendar.getInstance( ) ;
		Calendar endTime = Calendar.getInstance( ) ;

		if ( numDaysOffsetFromToday <= 0 ) {

			int fromDateOffSet = numberOfDaysToRetreive + numDaysOffsetFromToday ;
			int toDateOffSet = numDaysOffsetFromToday ;
			startTime.add( Calendar.DAY_OF_YEAR, -( fromDateOffSet ) ) ;
			endTime.add( Calendar.DAY_OF_YEAR, -( toDateOffSet ) ) ;

		} else {

			int fromDateOffSet = 0 ;
			int toDateOffSet = 0 ;

			if ( showDaysFrom ) {

				fromDateOffSet = numDaysOffsetFromToday ;
				toDateOffSet = numDaysOffsetFromToday - numberOfDaysToRetreive ;

			} else {

				fromDateOffSet = numDaysOffsetFromToday + numberOfDaysToRetreive ;
				toDateOffSet = numDaysOffsetFromToday ;

			}

			startTime.add( Calendar.DAY_OF_YEAR, -( fromDateOffSet ) ) ;
			endTime.add( Calendar.DAY_OF_YEAR, -( toDateOffSet ) ) ;

			endTime.set( Calendar.HOUR_OF_DAY, 0 ) ;
			endTime.set( Calendar.MINUTE, 0 ) ;
			endTime.set( Calendar.SECOND, 0 ) ;
			endTime.set( Calendar.MILLISECOND, 0 ) ;

			startTime.set( Calendar.HOUR_OF_DAY, 0 ) ;
			startTime.set( Calendar.MINUTE, 0 ) ;
			startTime.set( Calendar.SECOND, 0 ) ;
			startTime.set( Calendar.MILLISECOND, 0 ) ;

		}

		query.append( CREATED_ON + "." + LAST_UPDATED_ON, new Document( "$lte", endTime.getTime( ) ) ) ;
		return query ;

	}

	public MongoCollection<Document> getMongoEventCollection ( ) {

		return mongoClient.getDatabase( EVENT_DB_NAME )
				.getCollection( EVENT_COLLECTION_NAME )
				.withReadPreference( ReadPreference.secondaryPreferred( ) ) ;

	}

	private MongoCollection<Document> getMetricsMongoCollection ( ) {

		return mongoClient.getDatabase( METRICS_DB_NAME )
				.getCollection( METRICS_DATA_COLLECTION_NAME )
				.withReadPreference( ReadPreference.secondaryPreferred( ) ) ;

	}

	private MongoCollection<Document> getMetricsAttributeMongoCollection ( ) {

		return mongoClient.getDatabase( METRICS_DB_NAME )
				.getCollection( METRICS_ATTRIBUTES_COLLECTION_NAME )
				.withReadPreference( ReadPreference.secondaryPreferred( ) ) ;

	}

}
