package org.csap.events.db ;

import java.io.IOException ;
import java.util.List ;
import java.util.stream.Collectors ;

import javax.inject.Inject ;

import org.apache.commons.lang3.ArrayUtils ;
import org.bson.Document ;
import org.bson.json.JsonMode ;
import org.bson.json.JsonWriterSettings ;
import org.csap.events.CsapEventsApplication ;
import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapApplication ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.cache.annotation.Cacheable ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ArrayNode ;
import com.fasterxml.jackson.databind.node.ObjectNode ;

public class MetricsDataHandler {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Inject
	private MetricsDataReader metricsDataReader ;

	@Inject
	private AnalyticsHelper analyticsHelper ;

	@Inject
	private ObjectMapper jsonMapper ;

	JsonWriterSettings jsonWriter = JsonWriterSettings.builder( ).outputMode( JsonMode.RELAXED ).build( ) ;

	// storing JSON or BSON arrays in memory can get VERY expensive on large
	// datasets (millions of Objects created)
	// Only the unparsed string output is cached
	@Cacheable ( value = CsapEventsApplication.METRICS_REPORT_CACHE , key = "{#hostName,#collectionId,#numberOfDays,#dayOfYearAndServiceCacheKey,#appId,#life,#showDaysFrom,#padLatest}" )
	public String buildPerformanceGraphDataForToday (
														String hostName ,
														String collectionId ,
														String dayOfYearAndServiceCacheKey ,
														int numberOfDays ,
														int numDaysOffsetFromToday ,
														int bucketSize ,
														int bucketSpacing ,
														String[] serviceNameArray ,
														String appId ,
														String life ,
														boolean showDaysFrom ,
														boolean padLatest ) {

		return buildMetricsReportNoCache( hostName, collectionId, dayOfYearAndServiceCacheKey,
				numberOfDays, numDaysOffsetFromToday,
				bucketSize, bucketSpacing, serviceNameArray,
				appId, life, showDaysFrom, padLatest ) ;

	}

	@Cacheable ( value = CsapEventsApplication.HISTORICAL_REPORT_CACHE , key = "{#hostName,#collectionId,#numberOfDays,#dayOfYearAndServiceCacheKey,#appId,#life,#showDaysFrom,#padLatest}" )
	public String buildPerformanceGraphData (
												String hostName ,
												String collectionId ,
												String dayOfYearAndServiceCacheKey ,
												int numberOfDays ,
												int numDaysOffsetFromToday ,
												int bucketSize ,
												int bucketSpacing ,
												String[] serviceNameArray ,
												String appId ,
												String life ,
												boolean showDaysFrom ,
												boolean padLatest ) {

		return buildMetricsReportNoCache( hostName, collectionId, dayOfYearAndServiceCacheKey,
				numberOfDays, numDaysOffsetFromToday,
				bucketSize, bucketSpacing, serviceNameArray,
				appId, life, showDaysFrom, padLatest ) ;

	}

	public String buildMetricsReportNoCache (
												String hostName ,
												String collectionId ,
												String dayOfYearAndServiceCacheKey ,
												int numberOfDays ,
												int numDaysOffsetFromToday ,
												int bucketSize ,
												int bucketSpacing ,
												String[] serviceNamesRequested ,
												String appId ,
												String life ,
												boolean showDaysFrom ,
												boolean padLatest ) {

		logger.debug(
				"offset: {} No cache entry found or it expired - adding entry for Host: {}, type: {}, dayOfYearAndServiceCacheKey: {}, numberOfDays: {}",
				numDaysOffsetFromToday,
				hostName, collectionId, dayOfYearAndServiceCacheKey, numberOfDays ) ;

		var timerAll = metricUtilities.startTimer( ) ;
		var timerByResourceType = metricUtilities.startTimer( ) ;

		var timerByAppId = metricUtilities.startTimer( ) ;
		var timerByLifecycle = metricUtilities.startTimer( ) ;

		JsonNode allAttributes = findAttributesForRequest(
				hostName, collectionId,
				numberOfDays, numDaysOffsetFromToday,
				serviceNamesRequested, showDaysFrom ) ;

		ObjectNode csapGraphReport = jsonMapper.createObjectNode( ) ;

		if ( allAttributes == null ) {

			logger.warn(
					"Verify event publication on host: {} \n Failed to load attribute data for: offset: {}  type: {}, dayOfYearAndServiceCacheKey: {}, numberOfDays: {}",
					hostName,
					numDaysOffsetFromToday,
					collectionId, dayOfYearAndServiceCacheKey, numberOfDays ) ;
			csapGraphReport.put( "error", "Error getting data" ) ;
			return csapGraphReport.toString( ) ;

		}

		List<String> allAttributeNames = retrieveDataPointNames( allAttributes ) ;
		allAttributeNames.add( "timeStamp" ) ;

		long numDaysAvailable = metricsDataReader.retrieveNumDaysOfMetrics( hostName ) ;

		var filteredAttributes = filterGraphAttributes( collectionId, serviceNamesRequested, allAttributes, hostName ) ;

		( (ObjectNode) filteredAttributes ).put( "numDaysAvailable", numDaysAvailable ) ;

		try {

			// csapGraphReport.set( "attributes", jsonMapper.readTree(
			// graphAttributesFiltered.toJson( jsonWriter ) ) ) ;
			csapGraphReport.set( "attributes", filteredAttributes ) ;

		} catch ( Exception e ) {

			logger.error( "Failed parsing attributes: {}", CSAP.buildCsapStack( e ) ) ;

		}

		List<String> filteredAttributeNames = retrieveDataPointNames( filteredAttributes ) ;
		filteredAttributeNames.add( "timeStamp" ) ;
		// graphAttributeNames.add( "totalCpu" ) ;
		logger.debug( "data will be filtered to only contain: graphAttributeNames: {}", filteredAttributeNames ) ;

		// allAttributeNames or filteredAttributeNames
		ObjectNode allGraphData = metricsDataReader.findAndMergeMetricData(
				hostName, collectionId, numberOfDays, numDaysOffsetFromToday,
				appId, life, showDaysFrom, padLatest, filteredAttributeNames ) ;

		if ( bucketSize > 0 && bucketSpacing >= 1 ) {

			reduceDataBySampling( allGraphData, bucketSize, bucketSpacing ) ;

		}

		csapGraphReport.set( "data", allGraphData ) ;

		metricUtilities.stopTimer( timerByResourceType, "metrics-get.by-type." + collectionId ) ;
		metricUtilities.stopTimer( timerByAppId, "metrics-get.by-appid." + appId ) ;
		metricUtilities.stopTimer( timerByLifecycle, "metrics-get.by-life." + life ) ;
		metricUtilities.stopTimer( timerAll, "csap.metrics-get" ) ;

		// logger.info( "csapGraphReport: {}", CSAP.jsonPrint( csapGraphReport ) );

		// Performance Note:
		// storing JSON or BSON arrays in memory can get VERY expensive on large
		// datasets (millions of Objects created)
		// Only the unparsed string output is cached
		return csapGraphReport.toString( ) ;

	}

	private JsonNode findAttributesForRequest (
												String hostName ,
												String collectionId ,
												int numberOfDays ,
												int numDaysOffsetFromToday ,
												String[] serviceNamesRequested ,
												boolean showDaysFrom ) {

		JsonNode metricsAttributeDocumentWithServiceReq = null ;

		var requestedServiceNames = List.of( serviceNamesRequested ) ;
		//
		// Request day may not have data: iterate over range until found
		//
		var missingService = false ;

		for ( int dayAttempt = 0; dayAttempt < numberOfDays; dayAttempt++ ) {

			var offSetDays = numDaysOffsetFromToday + dayAttempt ;

			Document metricsAttributeDocument = metricsDataReader.getMetricsAttribute(
					hostName, collectionId, numberOfDays, offSetDays, showDaysFrom ) ;

			if ( null == metricsAttributeDocument ) {

				// return null ;
				continue ;

			}

			ObjectNode metricsAttributeReport = jsonMapper.createObjectNode( ) ;

			try {

				metricsAttributeReport = (ObjectNode) jsonMapper.readTree( metricsAttributeDocument.toJson(
						jsonWriter ) ) ;

			} catch ( IOException e ) {

				logger.error( "Failed parsing attributes: {}", CSAP.buildCsapStack( e ) ) ;

			}

			// even if this is not the final one - use it so common attributes are used
			metricsAttributeDocumentWithServiceReq = metricsAttributeReport ;

			var servicesAvailable = metricsAttributeReport.path( "servicesAvailable" ) ;
			logger.debug( "servicesAvailable: {}", servicesAvailable ) ;
			var requestedIsMissing = requestedServiceNames.stream( )
					.filter( reqService -> {

						var isInAvailable = CSAP.jsonStream( servicesAvailable )
								.map( JsonNode::asText )

								// .filter( reqService::equals )
								.filter( availableService -> {

									if ( availableService.equals( reqService ) )
										return true ;

									// legacy support for application collections
									if ( ( collectionId.startsWith( "jmx" )
											|| collectionId.startsWith( CsapApplication.COLLECTION_APPLICATION ) )
											&& availableService.startsWith( reqService ) )
										return true ;

									return false ;

								} )

								.findFirst( ) ;
						return isInAvailable.isEmpty( ) ;

					} )
					.findFirst( ) ;

			if ( requestedIsMissing.isPresent( ) ) {

				missingService = true ;

			} else {

				logger.debug( "{} Services requested: {} \n found in available: {}", collectionId,
						requestedServiceNames,
						servicesAvailable ) ;
				break ;

			}

		}

		if ( missingService ) {

			logger.warn( "{} Services requested not found on day requested: {}", collectionId, requestedServiceNames ) ;

		}

		return metricsAttributeDocumentWithServiceReq ;

	}

	// used for sampling data over large data sets
	private void reduceDataBySampling ( ObjectNode allGraphData , int bucketSize , int bucketSpacing ) {

		CSAP.asStreamHandleNulls( allGraphData ).forEach( attribute -> {

			var collectedValues = (ArrayNode) allGraphData.get( attribute ) ;

			ArrayNode sampledValues = jsonMapper.createArrayNode( ) ;

			int start = 0 ;
			int end = bucketSize - 1 ;
			boolean reInitializeRange = false ;

			for ( int k = 0; k < collectedValues.size( ); k++ ) {

				if ( k >= start && k <= end ) {

					reInitializeRange = true ;
					sampledValues.add( collectedValues.get( k ) ) ;

				} else if ( reInitializeRange ) {

					start = k + bucketSpacing ;
					end = ( k + bucketSpacing + bucketSize - 1 ) ;
					reInitializeRange = false ;

				}

			}

			allGraphData.set( attribute, sampledValues ) ;

		} ) ;

	}

	private JsonNode filterGraphAttributes (
												String id ,
												String[] serviceNamesRequested ,
												JsonNode allAttributes ,
												String host ) {

		var csapReportAttributes = allAttributes.deepCopy( ) ;

		var servicesAvailble = CSAP.jsonStream( csapReportAttributes.path( "servicesAvailable" ) )
				.map( JsonNode::asText )
				.collect( Collectors.toList( ) ) ;

		String[] serviceNamesFiltered = buildServiceNamesRequested(
				id, serviceNamesRequested,
				servicesAvailble, host ) ;

		if ( ArrayUtils.isNotEmpty( serviceNamesFiltered ) ) {

			var serviceNamesArray = ( (ObjectNode) csapReportAttributes ).putArray( "servicesRequested" ) ;

			List.of( serviceNamesFiltered ).stream( ).forEach( serviceNamesArray::add ) ;

			var graphsAttributes = csapReportAttributes.path( "graphs" ) ;

			CSAP.asStreamHandleNulls( graphsAttributes ).forEach( graphName -> {

				var graphTypes = graphsAttributes.path( graphName ) ;

				var graphTypesToRemove = CSAP.asStreamHandleNulls( graphTypes )
						.filter( graphType -> ! isServiceGraphRequired( id, serviceNamesFiltered, graphType ) )
						.collect( Collectors.toList( ) ) ;

				// logger.debug( "Items to be removed: {}", graphTypesToRemove ) ;

				graphTypesToRemove.stream( ).forEach( typeToRemove -> {

					( (ObjectNode) graphTypes ).remove( typeToRemove ) ;

				} ) ;

			} ) ;

		}

		return csapReportAttributes ;

	}

	private boolean isServiceGraphRequired ( String id , String[] serviceNameArray , String graphName ) {

		for ( String serviceName : serviceNameArray ) {

			if ( serviceName.trim( ).length( ) > 0 && graphName.contains( serviceName ) ) {

				return true ;

			}

		}

		if ( "timeStamp".equalsIgnoreCase( graphName )
				|| "totalCpu".equalsIgnoreCase( graphName )
				|| id.startsWith( CsapApplication.COLLECTION_APPLICATION )
				|| ( ( id.startsWith( "jmx" ) && ! id.startsWith( "jmx_" ) ) ) ) {

			return true ;

		}

		return false ;

	}

	private String[] buildServiceNamesRequested (
													String id ,
													String[] serviceNameArray ,
													List<String> servicesAvailable ,
													String host ) {

		String[] newServiceArray = null ;

		logger.debug( "id: {} serviceNameArray: {}", id, List.of( serviceNameArray ) ) ;

		if ( id.startsWith( "resource" ) || id.startsWith( CsapApplication.COLLECTION_HOST )
				|| ( id.startsWith( CsapApplication.COLLECTION_APPLICATION ) )
				|| ( ( id.startsWith( "jmx" ) && ! id.startsWith( "jmx_" ) ) ) ) {

			return newServiceArray ;

		} else if ( ArrayUtils.isNotEmpty( serviceNameArray ) ) {

			return serviceNameArray ;

		} else {

			List<String> topServices = analyticsHelper.getTopServices( host ) ;
			logger.debug( "top services{}", topServices ) ;

			if ( topServices.size( ) == 0 ) {

				topServices = servicesAvailable ;

			}

			// BasicDBList availableServices = (BasicDBList)
			// attribObject.get("servicesAvailable");
			if ( null != topServices ) {

				if ( topServices.size( ) < 5 ) {

					newServiceArray = new String[topServices.size( )] ;

				} else {

					newServiceArray = new String[5] ;

				}

				for ( int i = 0; i < newServiceArray.length; i++ ) {

					newServiceArray[i] = (String) topServices.get( i ) ;

				}

			}

		}

		return newServiceArray ;

	}

	private List<String> retrieveDataPointNames ( JsonNode metricReportAttributes ) {

		var graphsInAttributes = metricReportAttributes.path( "graphs" ) ;

		var graphColumnNames = CSAP.asStreamHandleNulls( graphsInAttributes )

				.flatMap( graphName -> CSAP.asStreamHandleNulls( graphsInAttributes.path( graphName ) ) )

				// fixed values are used by csap js code to generate lines; ignore items
				// prefixed
				.filter( attributeName -> ! attributeName.startsWith( "attributes_" ) )

				.collect( Collectors.toList( ) ) ;

		logger.debug( "graphColumnNames: {}", graphColumnNames ) ;
		return graphColumnNames ;

	}

}
