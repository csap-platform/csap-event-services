package org.csap.events.db ;

import static com.mongodb.client.model.Projections.excludeId ;
import static com.mongodb.client.model.Projections.fields ;
import static com.mongodb.client.model.Projections.include ;
import static org.csap.events.util.MetricsJsonConstants.APP_ID ;
import static org.csap.events.util.MetricsJsonConstants.CATEGORY ;
import static org.csap.events.util.MetricsJsonConstants.CREATED_ON ;
import static org.csap.events.util.MetricsJsonConstants.DATE ;
import static org.csap.events.util.MetricsJsonConstants.HOST ;
import static org.csap.events.util.MetricsJsonConstants.HOST_NAME ;
import static org.csap.events.util.MetricsJsonConstants.LIFE_CYCLE ;
import static org.csap.events.util.MetricsJsonConstants.METADATA ;
import static org.csap.events.util.MetricsJsonConstants.PROJECT ;
import static org.csap.events.util.MetricsJsonConstants.UIUSER ;

import java.io.IOException ;
import java.util.ArrayList ;
import java.util.Arrays ;
import java.util.Calendar ;
import java.util.Date ;
import java.util.HashMap ;
import java.util.HashSet ;
import java.util.List ;
import java.util.Map ;
import java.util.Set ;
import java.util.concurrent.TimeUnit ;
import java.util.concurrent.atomic.AtomicBoolean ;
import java.util.regex.Pattern ;
import java.util.stream.Collectors ;

import javax.inject.Inject ;

import org.apache.commons.lang3.ArrayUtils ;
import org.apache.commons.lang3.StringUtils ;
import org.apache.commons.lang3.math.NumberUtils ;
import org.bson.Document ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.EventJsonConstants ;
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
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.BasicDBObject ;
import com.mongodb.client.AggregateIterable ;

@Service
public class AnalyticsDbReader {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	public static int TREND_SAMPLES_FOR_24_HOURS = 48 ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Inject
	private AnalyticsHelper analyticsHelper ;

	@Inject
	private TrendingReportHelper trendingReportHelper ;

	public AggregateIterable<Document> userActivityReport (
															String appId ,
															String project ,
															String life ,
															int numDays ) {

		Document query = analyticsHelper.constructQuery( appId, project, life, numDays ) ;
		// query.append( METADATA + "." + UIUSER, new BasicDBObject( "$exists",
		// true ) );

		Pattern uiPattern = Pattern.compile( "^/csap/ui/" ) ;
		query.append( CATEGORY, uiPattern ) ;

		Document match = new Document( "$match", query ) ;

		Document groupFields = new Document( "_id", "$" + METADATA + "." + UIUSER ) ;
		groupFields.put( "totActivity", new BasicDBObject( "$sum", 1 ) ) ;
		Document group = new Document( "$group", groupFields ) ;

		Document projectFields = new Document( ) ;

		projectFields.append( UIUSER, "$_id" ) ;
		projectFields.append( "totActivity", "$totActivity" ) ;
		projectFields.append( "_id", 0 ) ;
		Document projectUiUser = new Document( "$project", projectFields ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( group ) ;
		operations.add( projectUiUser ) ;

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( operations ) ;

		logger.info( "Query: {}", operations ) ;

		return aggregationOutput ;

	}

	public AggregateIterable<Document> userActivityTrendingReport (
																	String appId ,
																	String project ,
																	String life ,
																	int numDays ,
																	int dateOffSet ) {

		var timer = metricUtilities.startTimer( ) ;
		List<Document> aggregationPipeline = new ArrayList<>( ) ;
		// category is passed as null so that it is not added to query
		Document query = constructDocumentQuery( appId, project, life, null ) ;
		query = addDateToDocumentQuery( query, numDays, dateOffSet ) ;
		// query.append( METADATA + "." + UIUSER, new Document( "$exists", true
		// ) );

		Pattern uiPattern = Pattern.compile( "^/csap/ui/" ) ;
		query.append( CATEGORY, uiPattern ) ;

		Document match = new Document( "$match", query ) ;
		aggregationPipeline.add( match ) ;

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( "appId", "$appId" ) ;
		groupFieldMap.put( "project", "$project" ) ;
		groupFieldMap.put( "lifecycle", "$lifecycle" ) ;
		groupFieldMap.put( "date", "$createdOn.date" ) ;
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;
		groupFields.put( "totActivity", new Document( "$sum", 1 ) ) ;
		Document group = new Document( "$group", groupFields ) ;
		aggregationPipeline.add( group ) ;

		Document sortOrder = new Document( ) ;
		sortOrder.append( "_id.date", 1 ) ;
		Document sort = new Document( "$sort", sortOrder ) ;
		aggregationPipeline.add( sort ) ;

		Map<String, Object> groupByDateMap = new HashMap<>( ) ;
		groupByDateMap.put( "appId", "$_id.appId" ) ;
		groupByDateMap.put( "project", "$_id.project" ) ;
		groupByDateMap.put( "lifecycle", "$_id.lifecycle" ) ;
		Document groupVmData = new Document( "_id", new Document( groupByDateMap ) ) ;
		groupVmData.append( "date", new Document( "$push", "$_id.date" ) ) ;
		groupVmData.append( "totActivity", new Document( "$push", "$totActivity" ) ) ;
		Document groupData = new Document( "$group", groupVmData ) ;
		aggregationPipeline.add( groupData ) ;

		Document projectOutput = new Document( ) ;
		projectOutput.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", 1 )
				.append( "totActivity", 1 )
				.append( "_id", 0 ) ;
		Document projectOutputData = new Document( "$project", projectOutput ) ;

		aggregationPipeline.add( projectOutputData ) ;

		if ( StringUtils.isBlank( project ) ) {

			Document sortByProjectOrder = new Document( ) ;
			sortByProjectOrder.append( "project", 1 ) ;
			Document sortByProject = new Document( "$sort", sortByProjectOrder ) ;
			aggregationPipeline.add( sortByProject ) ;

		}

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( aggregationPipeline ) ;

		var nanos = metricUtilities.stopTimer( timer, "userReportTrending" ) ;

		logger.info( "pipeline: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ), aggregationPipeline ) ;

		return aggregationOutput ;

	}

	// https://docs.mongodb.org/getting-started/java/client/
	public JsonNode buildCategoryTrendingReport (
													boolean byHost ,
													String appId ,
													String project ,
													String life ,
													String serviceNameFilter ,
													String category ,
													String[] metricsId ,
													String[] divideBy ,
													String allVmTotal ,
													int numDays ,
													int dateOffSet )
		throws Exception {

		int dayCount = Math.abs( numDays ) ;

		var timer = metricUtilities.startTimer( ) ;

		logger.debug( "Trending report category {} serviceNameFilter: {}", category, serviceNameFilter ) ;

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate(
						trendingReportHelper.trendingOperationPipelineBuilder(
								byHost,
								appId, project, life, serviceNameFilter,
								category, metricsId, divideBy,
								allVmTotal, dayCount, dateOffSet ) ) ;

		var trendReport = EventJsonConstants.transformToJackson( aggregationOutput ) ;

		metricUtilities.stopTimer( timer, "trendingByCategory" ) ;

		if ( numDays < 0 ) {

			buildCategoryHourlyReport( byHost, appId, project, life, serviceNameFilter, category, metricsId, divideBy,
					dateOffSet, dayCount,
					trendReport ) ;

		}

		return trendReport ;

	}

	private void buildCategoryHourlyReport (
												boolean byHost ,
												String appId ,
												String project ,
												String life ,
												String serviceNameFilter ,
												String category ,
												String[] metricsId ,
												String[] divideBy ,
												int dateOffSet ,
												int dayCount ,
												JsonNode trendReport )
		throws IOException {
		// build the hourly trends

		var trendsWithHostReported = trendReport ;

		if ( ! byHost ) {

			var perHostForReport = true ;
			AggregateIterable<Document> aggregationOutput2 = analyticsHelper.getMongoEventCollection( )
					.aggregate(
							trendingReportHelper.trendingOperationPipelineBuilder(
									perHostForReport,
									appId, project, life, serviceNameFilter,
									category, metricsId, divideBy,
									Boolean.toString( ! perHostForReport ), dayCount, dateOffSet ) ) ;

			trendsWithHostReported = EventJsonConstants.transformToJackson( aggregationOutput2 ) ;

		}

		logger.debug( "trendsWithHostReported: {}", trendsWithHostReported ) ;

		if ( StringUtils.isEmpty( trendsWithHostReported.path( 0 ).path( "host" ).asText( ) ) ) {

			logger.info( "{}: host data not available for report,  service: {}, metric: {} ", category,
					serviceNameFilter, metricsId ) ;
			return ;

		}

		// if ( StringUtils.isEmpty( serviceNameFilter) ) {
		// serviceNameFilter = "" ;
		// }
		// List<String> services = List.of( serviceNameFilter ) ;
		Map<String, JsonNode> hostMetricReports = CSAP.jsonStream( trendsWithHostReported )
				.map( hostTrendReport -> hostTrendReport.path( "host" ) )
				.filter( JsonNode::isTextual )
				.map( JsonNode::asText )
				.map( host -> {

					var metricReport = metricsId[0] ;
					logger.debug( "{} report for host: {}, service: {}, metric: {} ", category, host, serviceNameFilter,
							metricReport ) ;

					var metricsReportId = "application-" + serviceNameFilter + "_30" ;
					var metricSource = metricsId[0] ;

					if ( category.contains( "os-process" ) ) {

						metricSource = metricsId[0] + "_" + serviceNameFilter ;
						metricsReportId = "os-process_30" ;

					} else if ( category.contains( "java" ) ) {

						metricSource = metricsId[0] + "_" + serviceNameFilter ;
						metricsReportId = "java_30" ;

					}

					var hostMetricReport = jsonMapper.createObjectNode( ) ;
					hostMetricReport.put( "host", host ) ;

					try {

						String[] services = null ;

						if ( StringUtils.isNotEmpty( serviceNameFilter ) ) {

							services = List.of( serviceNameFilter ).toArray( new String[0] ) ;

						}

						var hostFullReport = jsonMapper.readTree( metricsDataHandler
								.buildMetricsReportNoCache(
										host, metricsReportId,
										"not-cached",
										dayCount, dateOffSet,
										1, 0,
										services,
										appId, life, false, false ) ) ;

						logger.debug( "hostFullReport: {}", hostFullReport ) ;

						hostMetricReport.set( "timeStamp",
								reduce_using_samples(
										hostFullReport.path( "data" ).path( "timeStamp" ),
										TREND_SAMPLES_FOR_24_HOURS,
										false, false ) ) ;

						var metricData = hostFullReport.path( "data" ).path( metricSource ) ;
						var useTotals = new AtomicBoolean( false ) ;

						if ( metricData.isMissingNode( ) ) {

							var pods = hostFullReport.path( "attributes" ).path( "servicesAvailable" ) ;
							var averagedData = jsonMapper.createArrayNode( ) ;
							CSAP.jsonStream( pods )
									.filter( JsonNode::isTextual )
									.map( podNode -> podNode.asText( ) )
									.forEach( podName -> {

										logger.debug( "loading pod data: {}", podName ) ;
										var podFilter = metricsId[0] + "_" + podName ;
										var podData = hostFullReport.path( "data" ).path( podFilter ) ;

										if ( averagedData.isEmpty( ) ) {

											averagedData.addAll( (ArrayNode) podData ) ;

										} else {

											if ( podData.path( 0 ).isInt( ) ) {

												useTotals.set( true ) ;
												var mergedVals = jsonMapper.createArrayNode( ) ;

												for ( var i = 0; i < podData.size( ); i++ ) {

													var sum = podData.path( i ).asLong( ) + averagedData.path( i )
															.asLong( ) ;
													mergedVals.add( sum ) ;

												}

												averagedData.removeAll( ) ;
												averagedData.addAll( mergedVals ) ;

											} else {

												logger.warn( "pod value {} is not a int - skipping merge: {}",
														podFilter, podData.path( 0 ) ) ;

											}

										}

									} ) ;

							metricData = averagedData ;

						}

						hostMetricReport.set( metricReport,
								reduce_using_samples(
										metricData,
										TREND_SAMPLES_FOR_24_HOURS,
										true, useTotals.get( ) ) ) ;

					} catch ( Exception e ) {

						logger.warn( "Failed loading metrics: {} {}", host, CSAP.buildCsapStack( e ) ) ;

					}

					return hostMetricReport ;

				} )
				.collect( Collectors.toMap(
						hostMetricReport -> hostMetricReport.get( "host" ).asText( ),
						hostMetricReport -> hostMetricReport ) ) ;

		logger.debug( "hostMetricReports: {}", hostMetricReports ) ;

		if ( byHost ) {

			// insert data directly from metrics
			CSAP.jsonStream( trendReport )
					.filter( JsonNode::isObject )
					.map( hostTrendReport -> (ObjectNode) hostTrendReport )
					.forEach( hostTrendReport -> {

						var host = hostTrendReport.path( "host" ).asText( ) ;
						var hostMetricReport = hostMetricReports.get( host ) ;

						if ( hostMetricReport != null ) {

							hostTrendReport.set( "timeStamp", hostMetricReport.path( "timeStamp" ) ) ;
							hostTrendReport.set( metricsId[0], hostMetricReport.path( metricsId[0] ) ) ;

						} else {

							logger.warn( "no metrics for host: {}", host ) ;

						}

					} ) ;

		} else {

			// average all the data
			// CSAP.jsonStream( hostMetricReports ) ;
			var averagedData = jsonMapper.createArrayNode( ) ;
			var longestTimeStamp = jsonMapper.createArrayNode( ) ;
			hostMetricReports.values( ).stream( )
					.filter( JsonNode::isObject )
					.map( hostTrendReport -> (ObjectNode) hostTrendReport )
					.forEach( trendData -> {

						// timestamps
						var timeValues = trendData.path( "timeStamp" ) ;

						if ( timeValues.size( ) > longestTimeStamp.size( ) ) {

							longestTimeStamp.removeAll( ) ;
							longestTimeStamp.addAll( (ArrayNode) timeValues ) ;

						}

						var dataValues = trendData.path( metricsId[0] ) ;

						if ( averagedData.isEmpty( ) ) {

							averagedData.addAll( (ArrayNode) dataValues ) ;

						} else {

							if ( dataValues.path( 0 ).isInt( ) ) {

								var mergedVals = jsonMapper.createArrayNode( ) ;

								for ( var i = 0; i < dataValues.size( ); i++ ) {

									var sum = dataValues.path( i ).asLong( ) + averagedData.path( i ).asLong( ) ;
									mergedVals.add( sum ) ;

								}

								averagedData.removeAll( ) ;
								averagedData.addAll( mergedVals ) ;

							} else {

								logger.warn( "{} is not a int - skipping merge: '{}'", metricsId[0], dataValues.path(
										0 ) ) ;

							}

						}

					} ) ;

			var firstEntry = (ObjectNode) trendReport.path( 0 ) ;
			firstEntry.set( "timeStamp", longestTimeStamp ) ;
			firstEntry.set( metricsId[0], averagedData ) ;

		}

	}

	public AggregateIterable<Document> findSummaryReportDataUsingCategory (
																			String appId ,
																			String project ,
																			String life ,
																			String hostName ,
																			String serviceNameFilter ,
																			String category ,
																			int numDays ,
																			int dateOffSet ) {

		logger.debug( "{} Report for project: {}, number of Days: {} , offSet: {}",
				category, project, numDays, dateOffSet ) ;

		List<Document> operations = new ArrayList<>( ) ;
		logger.debug( "category {} ", category ) ;
		Document query = constructDocumentQuery( appId, project, life, category ) ;

		if ( StringUtils.isNotBlank( hostName ) ) {

			query.append( HOST, hostName ) ;

		}

		query = addDateToDocumentQuery( query, numDays, dateOffSet ) ;

		Document match = new Document( "$match", query ) ;
		operations.add( match ) ;

		Document unwind = new Document( "$unwind", "$data.summary" ) ;
		operations.add( unwind ) ;

		String attributesService = EventJsonConstants.CSAP_AGENT ;// use csagent attributes

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			attributesService = serviceNameFilter ;
			Document serviceNameQuery = new Document( ) ;
			serviceNameQuery.append( "data.summary.serviceName", serviceNameFilter ) ;
			Document serviceNameMatch = new Document( "$match", serviceNameQuery ) ;
			operations.add( serviceNameMatch ) ;

		}

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( "serviceName", "$data.summary.serviceName" ) ;
		groupFieldMap.put( APP_ID, "$" + APP_ID ) ;
		groupFieldMap.put( PROJECT, "$" + PROJECT ) ;
		groupFieldMap.put( LIFE_CYCLE, "$" + LIFE_CYCLE ) ;

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;
		Document projectFields = new Document( ) ;
		projectFields
				.append( APP_ID, "$_id." + APP_ID )
				.append( LIFE_CYCLE, "$_id." + LIFE_CYCLE )
				.append( PROJECT, "$_id." + PROJECT )
				.append( "serviceName", "$_id.serviceName" )
				.append( "_id", 0 ) ;
		Set<String> keys = analyticsHelper.findReportDocumentAttributes( appId, project, life, category,
				attributesService ) ;
		if ( null == keys )
			keys = new HashSet<>( ) ;

		/*
		 * keys.stream().filter(key -> !"serviceName".equalsIgnoreCase(key)).forEach(key
		 * -> {
		 * 
		 * });
		 */
		for ( String key : keys ) {

			if ( "serviceName".equalsIgnoreCase( key ) )
				continue ;

			if ( key.endsWith( "Avg" ) ) {

				groupFields.append( key, new Document( "$avg", "$data.summary." + key ) ) ;

			} else {

				groupFields.append( key, new Document( "$sum", "$data.summary." + key ) ) ;

			}

			projectFields.append( key, 1 ) ;

		}

		Document group = new Document( "$group", groupFields ) ;
		Document projectHost = new Document( "$project", projectFields ) ;

		operations.add( group ) ;
		operations.add( projectHost ) ;

		logger.debug( "Aggregation: {}", operations ) ;

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( operations ) ;
		return aggregationOutput ;

	}

	public AggregateIterable<Document> findDetailReportDataUsingCategory (
																			String appId ,
																			String project ,
																			String life ,
																			String serviceNameFilter ,
																			String category ,
																			int numDays ,
																			int dateOffSet ) {

		List<Document> operations = new ArrayList<>( ) ;

		Document query = constructDocumentQuery( appId, project, life, category ) ;
		query = addDateToDocumentQuery( query, numDays, dateOffSet ) ;
		logger.debug( "query::{}", query ) ;
		Document match = new Document( "$match", query ) ;
		operations.add( match ) ;

		Document unwind = new Document( "$unwind", "$data.summary" ) ;
		operations.add( unwind ) ;

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			Document serviceNameQuery = new Document( ) ;
			serviceNameQuery.append( "data.summary.serviceName", serviceNameFilter ) ;
			Document serviceNameMatch = new Document( "$match", serviceNameQuery ) ;
			operations.add( serviceNameMatch ) ;

		}

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( "serviceName", "$data.summary.serviceName" ) ;
		groupFieldMap.put( APP_ID, "$" + APP_ID ) ;
		groupFieldMap.put( PROJECT, "$" + PROJECT ) ;
		groupFieldMap.put( LIFE_CYCLE, "$" + LIFE_CYCLE ) ;
		groupFieldMap.put( HOST, "$" + HOST ) ;

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;
		Document projectFields = new Document( ) ;
		projectFields
				.append( APP_ID, "$_id." + APP_ID )
				.append( LIFE_CYCLE, "$_id." + LIFE_CYCLE )
				.append( PROJECT, "$_id." + PROJECT )
				.append( "serviceName", "$_id.serviceName" )
				.append( HOST, "$_id." + HOST )
				.append( "_id", 0 ) ;
		Set<String> keys = analyticsHelper.findReportDocumentAttributes( appId, project, life, category,
				serviceNameFilter ) ;
		if ( null == keys )
			keys = new HashSet<>( ) ;

		for ( String key : keys ) {

			if ( "serviceName".equalsIgnoreCase( key ) )
				continue ;

			if ( key.endsWith( "Avg" ) ) {

				groupFields.append( key, new BasicDBObject( "$avg", "$data.summary." + key ) ) ;

			} else {

				groupFields.append( key, new BasicDBObject( "$sum", "$data.summary." + key ) ) ;

			}

			projectFields.append( key, 1 ) ;

		}

		Document group = new Document( "$group", groupFields ) ;
		Document projectHost = new Document( "$project", projectFields ) ;

		operations.add( group ) ;
		operations.add( projectHost ) ;

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( operations ) ;
		return aggregationOutput ;

	}

	public AggregateIterable<Document> getVmHealthReport (
															String appId ,
															String project ,
															String life ,
															boolean perVm ,
															int top ,
															int low ,
															int numDays ,
															int dateOffSet ,
															String category ) {

		var timer = metricUtilities.startTimer( ) ;
		List<Document> aggregationPipeline = new ArrayList<>( ) ;
		Set matchHosts = new HashSet( ) ;

		if ( top > 0 ) {

			List topHosts = trendingReportHelper.topHosts( appId, project, life, new String[] {
					"UnHealthyEventCount"
			},
					null, top, -1,
					category, "", false, numDays, dateOffSet ) ;
			logger.debug( "top hosts {} ", topHosts ) ;
			matchHosts.addAll( topHosts ) ;

		}

		if ( low > 0 ) {

			List lowHosts = trendingReportHelper.topHosts( appId, project, life, new String[] {
					"UnHealthyEventCount"
			},
					null, low, 1,
					category, "", false, numDays, dateOffSet ) ;
			matchHosts.addAll( lowHosts ) ;

		}

		Document query = constructDocumentQuery( appId, project, life, category ) ;
		query = addDateToDocumentQuery( query, numDays, dateOffSet ) ;

		if ( matchHosts.size( ) > 0 ) {

			Document hostMatch = new Document( "$in", matchHosts ) ;
			query.append( "host", hostMatch ) ;

		}

		Document match = new Document( "$match", query ) ;
		aggregationPipeline.add( match ) ;

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( "appId", "$appId" ) ;
		groupFieldMap.put( "project", "$project" ) ;
		groupFieldMap.put( "lifecycle", "$lifecycle" ) ;
		groupFieldMap.put( "date", "$createdOn.date" ) ;

		if ( perVm ) {

			groupFieldMap.put( "host", "$host" ) ;

		}

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;
		groupFields.append( "UnHealthyCount", new Document( "$sum", "$data.UnHealthyEventCount" ) ) ;
		Document group = new Document( "$group", groupFields ) ;
		aggregationPipeline.add( group ) ;

		Document sortOrder = new Document( ) ;
		sortOrder.append( "_id.date", 1 ) ;
		Document sort = new Document( "$sort", sortOrder ) ;
		aggregationPipeline.add( sort ) ;

		Map<String, Object> groupByDateMap = new HashMap<>( ) ;
		groupByDateMap.put( "appId", "$_id.appId" ) ;
		groupByDateMap.put( "project", "$_id.project" ) ;
		groupByDateMap.put( "lifecycle", "$_id.lifecycle" ) ;

		if ( perVm ) {

			groupByDateMap.put( "host", "$_id.host" ) ;

		}

		Document groupVmData = new Document( "_id", new Document( groupByDateMap ) ) ;
		groupVmData.append( "date", new Document( "$push", "$_id.date" ) ) ;
		groupVmData.append( "UnHealthyCount", new Document( "$push", "$UnHealthyCount" ) ) ;

		Document groupData = new Document( "$group", groupVmData ) ;
		aggregationPipeline.add( groupData ) ;

		Document projectOutput = new Document( ) ;
		projectOutput.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", 1 )
				.append( "UnHealthyCount", 1 )
				.append( "_id", 0 ) ;

		if ( perVm ) {

			projectOutput.append( "host", "$_id.host" ) ;

		}

		Document projectOutputData = new Document( "$project", projectOutput ) ;
		aggregationPipeline.add( projectOutputData ) ;

		if ( StringUtils.isBlank( project ) ) {

			Document sortByProjectOrder = new Document( ) ;
			sortByProjectOrder.append( "project", 1 ) ;
			Document sortByProject = new Document( "$sort", sortByProjectOrder ) ;
			aggregationPipeline.add( sortByProject ) ;

		}

		logger.debug( "aggregation pipe line {}", aggregationPipeline ) ;
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( aggregationPipeline ) ;

		metricUtilities.stopTimer( timer, "vmHealthReport" ) ;
		return aggregationOutput ;

	}

	final static String CORES_USED = "coresUsed" ;

	public JsonNode buildCoreUsedTrendingReport (
													String appId ,
													String project ,
													String life ,
													String[] metricsId ,
													boolean perVm ,
													int top ,
													int low ,
													int numDays ,
													int dateOffSet )
		throws Exception {

		int dayCount = Math.abs( numDays ) ;
		boolean reportHostUsage = perVm ;

		AggregateIterable<Document> aggregationOutput = buildCoreTrendingDailyReport( appId, project, life, metricsId,
				perVm, top, low,
				dateOffSet, dayCount ) ;

		var trendReport = EventJsonConstants.transformToJackson( aggregationOutput ) ;

		if ( numDays < 0 ) {
			// build the hourly trends using usrCpu

			buildCoreTrendingHourlyReport( appId, project, life, metricsId, perVm, top, low, dateOffSet, dayCount,
					reportHostUsage,
					trendReport ) ;

		}

		return trendReport ;

	}

	private void buildCoreTrendingHourlyReport (
													String appId ,
													String project ,
													String life ,
													String[] metricsId ,
													boolean perVm ,
													int top ,
													int low ,
													int dateOffSet ,
													int dayCount ,
													boolean reportHostUsage ,
													JsonNode trendReport )
		throws IOException {

		var trendsWithHostReported = trendReport ;

		if ( ! perVm ) {

			var perHostForReport = true ;
			AggregateIterable<Document> aggregationOutput = buildCoreTrendingDailyReport(
					appId, project, life, metricsId, perHostForReport, top, low,
					dateOffSet, dayCount ) ;

			trendsWithHostReported = EventJsonConstants.transformToJackson( aggregationOutput ) ;

			// trendReport = trendsWithHostReported ;
		}

		var services = new String[0] ;

		Map<String, JsonNode> hostMetricReports = CSAP.jsonStream( trendsWithHostReported )
				.map( hostTrendReport -> hostTrendReport.path( "host" ) )
				.filter( JsonNode::isTextual )
				.map( JsonNode::asText )
				.map( host -> {

					logger.debug( "metric report for host: {} ", host ) ;

					var hostMetricReport = jsonMapper.createObjectNode( ) ;
					hostMetricReport.put( "host", host ) ;

					try {

						var hostFullReport = jsonMapper.readTree( metricsDataHandler
								.buildMetricsReportNoCache(
										host, "host_30",
										"not-cached",
										dayCount, dateOffSet,
										0, 0,
										services, appId, life, false, false ) ) ;

						hostMetricReport.set( "timeStamp",
								reduce_using_samples(
										hostFullReport.path( "data" ).path( "timeStamp" ),
										TREND_SAMPLES_FOR_24_HOURS,
										false, false ) ) ;

						var usrCpu = reduce_using_samples( hostFullReport.path( "data" ).path( "usrCpu" ),
								TREND_SAMPLES_FOR_24_HOURS,
								true, false ) ;
						// logger.debug("usrCpu: {}", usrCpu) ;

						var sysCpu = reduce_using_samples( hostFullReport.path( "data" ).path( "sysCpu" ),
								TREND_SAMPLES_FOR_24_HOURS,
								true, false ) ;

						var coresUsed = jsonMapper.createArrayNode( ) ;
						var cpuCount = hostFullReport.path( "attributes" ).path( "cpuCount" ).asInt( ) ;

						for ( var i = 0; i < usrCpu.size( ); i++ ) {

							double cores = ( usrCpu.path( i ).asDouble( ) + sysCpu.path( i ).asDouble( ) ) * cpuCount
									/ 100 ;
							coresUsed.add( CSAP.roundIt( cores, 2 ) ) ;

						}

						hostMetricReport.set( CORES_USED, coresUsed ) ;

					} catch ( Exception e ) {

						logger.warn( "Failed loading metrics: {} {}", host, CSAP.buildCsapStack( e ) ) ;

					}

					return hostMetricReport ;

				} )
				.collect( Collectors.toMap(
						hostMetricReport -> hostMetricReport.get( "host" ).asText( ),
						hostMetricReport -> hostMetricReport ) ) ;

		logger.debug( "hostMetricReports: {}", hostMetricReports ) ;

		if ( reportHostUsage ) {

			// insert data directly from metrics
			// insert using metric report
			CSAP.jsonStream( trendReport )
					.filter( JsonNode::isObject )
					.map( hostTrendReport -> (ObjectNode) hostTrendReport )
					.forEach( hostTrendReport -> {

						var host = hostTrendReport.path( "host" ).asText( ) ;
						var hostMetricReport = hostMetricReports.get( host ) ;

						if ( hostMetricReport != null ) {

							hostTrendReport.set( "timeStamp", hostMetricReport.path( "timeStamp" ) ) ;
							hostTrendReport.set( CORES_USED, hostMetricReport.path( CORES_USED ) ) ;

						} else {

							logger.warn( "no metrics for host: {}", host ) ;

						}

					} ) ;

		} else {

			// average all the data
			// CSAP.jsonStream( hostMetricReports ) ;
			var averagedData = jsonMapper.createArrayNode( ) ;
			var longestTimeStamp = jsonMapper.createArrayNode( ) ;
			hostMetricReports.values( ).stream( )
					.filter( JsonNode::isObject )
					.map( hostTrendReport -> (ObjectNode) hostTrendReport )
					.forEach( trendData -> {

						// timestamps
						var timeValues = trendData.path( "timeStamp" ) ;

						if ( timeValues.size( ) > longestTimeStamp.size( ) ) {

							longestTimeStamp.removeAll( ) ;
							longestTimeStamp.addAll( (ArrayNode) timeValues ) ;

						}

						var dataValues = trendData.path( CORES_USED ) ;

						if ( averagedData.isEmpty( ) ) {

							averagedData.addAll( (ArrayNode) dataValues ) ;

						}

					} ) ;

			var firstEntry = (ObjectNode) trendReport.path( 0 ) ;
			firstEntry.set( "timeStamp", longestTimeStamp ) ;
			firstEntry.set( CORES_USED, averagedData ) ;

		}

	}

	private AggregateIterable<Document> buildCoreTrendingDailyReport (
																		String appId ,
																		String project ,
																		String life ,
																		String[] metricsId ,
																		boolean perVm ,
																		int top ,
																		int low ,
																		int dateOffSet ,
																		int dayCount ) {

		var timer = metricUtilities.startTimer( ) ;
		String category = "/csap/reports/host/daily" ;
		Set matchHosts = new HashSet( ) ;

		if ( top > 0 ) {

			List topHosts = trendingReportHelper.topHosts( appId, project, life, metricsId,
					new String[] {
							"numberOfSamples"
					}, top, -1,
					category, "", false, dayCount, dateOffSet ) ;
			// logger.debug("top Hosts {} ",topHosts);
			// matchHosts.addAll(topHosts(appId, project, life,top, -1, numDays,
			// dateOffSet));
			matchHosts.addAll( topHosts ) ;

		}

		if ( low > 0 ) {

			// matchHosts.addAll(topHosts(appId, project, life,low, 1, numDays,
			// dateOffSet));
			List lowHosts = trendingReportHelper.topHosts( appId, project, life, metricsId,
					new String[] {
							"numberOfSamples"
					}, low, 1,
					category, "", false, dayCount, dateOffSet ) ;
			matchHosts.addAll( lowHosts ) ;

		}

		List<Document> operations = new ArrayList<>( ) ;

		Document query = constructDocumentQuery( appId, project, life, category ) ;
		query = addDateToDocumentQuery( query, dayCount, dateOffSet ) ;

		if ( matchHosts.size( ) > 0 ) {

			Document hostMatch = new Document( "$in", matchHosts ) ;
			query.append( "host", hostMatch ) ;

		}

		Document match = new Document( "$match", query ) ;
		operations.add( match ) ;

		Map<String, Object> groupFieldByHostMap = new HashMap<>( ) ;
		groupFieldByHostMap.put( "appId", "$appId" ) ;
		groupFieldByHostMap.put( "project", "$project" ) ;
		groupFieldByHostMap.put( "lifecycle", "$lifecycle" ) ;
		groupFieldByHostMap.put( "host", "$host" ) ;
		groupFieldByHostMap.put( "date", "$createdOn.date" ) ;
		Document groupFieldsByHost = new Document( "_id", new Document( groupFieldByHostMap ) ) ;

		for ( String key : metricsId ) {

			groupFieldsByHost.append( key, new Document( "$sum", "$data.summary." + key ) ) ;

		}

		groupFieldsByHost.append( "cpuCountAvg", new BasicDBObject( "$sum", "$data.summary.cpuCountAvg" ) ) ;
		groupFieldsByHost.append( "numberOfSamples", new BasicDBObject( "$sum", "$data.summary.numberOfSamples" ) ) ;
		Document groupByHost = new Document( "$group", groupFieldsByHost ) ;
		operations.add( groupByHost ) ;

		Document projectFields = new Document( ) ;
		List<Object> metricsList = new ArrayList<>( ) ;

		for ( String key : metricsId ) {

			metricsList.add( "$" + key ) ;
			projectFields.append( key, 1 ) ;

		}

		projectFields.append( "cpuCountAvg", 1 ).append( "numberOfSamples", 1 ) ;
		projectFields.append( "total", new Document( "$add", metricsList ) ) ;
		projectFields.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", "$_id.date" )
				.append( "host", "$_id.host" )
				.append( "_id", 0 ) ;
		Document projectData = new Document( "$project", projectFields ) ;
		operations.add( projectData ) ;

		Document numSampleProjection = new Document( ) ;
		List<Object> numSampleDivideList = new ArrayList<>( ) ;
		numSampleDivideList.add( "$total" ) ;
		numSampleDivideList.add( "$numberOfSamples" ) ;
		numSampleProjection.append( "appId", 1 )
				.append( "lifecycle", 1 )
				.append( "project", 1 )
				.append( "cpuCountAvg", 1 )
				.append( "numberOfSamples", 1 )
				.append( "date", 1 )
				.append( "host", 1 )
				.append( "totalSysCpu", 1 )
				.append( "totalUsrCpu", 1 ) ;

		numSampleProjection.append( "total", new Document( "$divide", numSampleDivideList ) ) ;
		Document projectNumSampleByHost = new Document( "$project", numSampleProjection ) ;
		operations.add( projectNumSampleByHost ) ;

		Document projectCoresUsed = new Document( ) ;
		List<Object> coresOpList = new ArrayList<>( ) ;
		coresOpList.add( "$total" ) ;
		coresOpList.add( "$cpuCountAvg" ) ;
		projectCoresUsed.append( "appId", 1 )
				.append( "lifecycle", 1 )
				.append( "project", 1 )
				.append( "cpuCountAvg", 1 )
				.append( "date", 1 )
				.append( "host", 1 )
				.append( "totalSysCpu", 1 )
				.append( "totalUsrCpu", 1 ) ;
		projectCoresUsed.append( CORES_USED, new Document( "$multiply", coresOpList ) ) ;
		Document projectCoresUsedData = new Document( "$project", projectCoresUsed ) ;
		operations.add( projectCoresUsedData ) ;

		Document projectCoresUsedPercent = new Document( ) ;
		List<Object> coresDivideList = new ArrayList<>( ) ;
		coresDivideList.add( "$coresUsed" ) ;
		coresDivideList.add( 100 ) ;
		projectCoresUsedPercent.append( "appId", 1 )
				.append( "lifecycle", 1 )
				.append( "project", 1 )
				.append( "cpuCountAvg", 1 )
				.append( "date", 1 )
				.append( "host", 1 )
				.append( "totalSysCpu", 1 )
				.append( "totalUsrCpu", 1 ) ;
		projectCoresUsedPercent.append( "coresUsedPercent", new Document( "$divide", coresDivideList ) ) ;
		Document projectCoresUsedPercentHost = new Document( "$project", projectCoresUsedPercent ) ;
		operations.add( projectCoresUsedPercentHost ) ;

		if ( ! perVm ) {

			Map<String, Object> groupByLifeMap = new HashMap<>( ) ;
			groupByLifeMap.put( "appId", "$appId" ) ;
			groupByLifeMap.put( "project", "$project" ) ;
			groupByLifeMap.put( "lifecycle", "$lifecycle" ) ;
			groupByLifeMap.put( "date", "$date" ) ;
			Document groupLifeData = new Document( "_id", new Document( groupByLifeMap ) ) ;
			groupLifeData.append( "coresUsedLife", new Document( "$sum", "$coresUsedPercent" ) ) ;
			groupLifeData.append( "totalSysCpu", new Document( "$sum", "$totalSysCpu" ) ) ;
			groupLifeData.append( "totalUsrCpu", new Document( "$sum", "$totalUsrCpu" ) ) ;
			groupLifeData.append( "cpuCountAvg", new Document( "$sum", "$cpuCountAvg" ) ) ;
			Document groupByLife = new Document( "$group", groupLifeData ) ;
			operations.add( groupByLife ) ;

		}

		Document sortOrder = new Document( ) ;

		if ( perVm ) {

			sortOrder.append( "date", 1 ) ;

		} else {

			sortOrder.append( "_id.date", 1 ) ;

		}

		Document sort = new Document( "$sort", sortOrder ) ;
		operations.add( sort ) ;

		if ( perVm ) {

			Map<String, Object> groupByDateMap = new HashMap<>( ) ;
			groupByDateMap.put( "appId", "$appId" ) ;
			groupByDateMap.put( "project", "$project" ) ;
			groupByDateMap.put( "host", "$host" ) ;
			groupByDateMap.put( "lifecycle", "$lifecycle" ) ;

			Document groupVmData = new Document( "_id", new Document( groupByDateMap ) ) ;
			groupVmData.append( "date", new Document( "$push", "$date" ) ) ;

			for ( String key : metricsId ) {

				groupVmData.append( key, new Document( "$push", "$" + key ) ) ;

			}

			groupVmData.append( "cpuCountAvg", new Document( "$push", "$cpuCountAvg" ) ) ;
			groupVmData.append( CORES_USED, new Document( "$push", "$coresUsedPercent" ) ) ;
			Document groupData = new Document( "$group", groupVmData ) ;
			operations.add( groupData ) ;

		} else {

			Map<String, Object> groupByDateMap = new HashMap<>( ) ;
			groupByDateMap.put( "appId", "$_id.appId" ) ;
			groupByDateMap.put( "project", "$_id.project" ) ;
			groupByDateMap.put( "lifecycle", "$_id.lifecycle" ) ;

			Document groupVmData = new Document( "_id", new Document( groupByDateMap ) ) ;
			groupVmData.append( "date", new Document( "$push", "$_id.date" ) ) ;

			for ( String key : metricsId ) {

				groupVmData.append( key, new Document( "$push", "$" + key ) ) ;

			}

			groupVmData.append( "cpuCountAvg", new Document( "$push", "$cpuCountAvg" ) ) ;
			groupVmData.append( CORES_USED, new Document( "$push", "$coresUsedLife" ) ) ;
			Document groupData = new Document( "$group", groupVmData ) ;
			operations.add( groupData ) ;

		}

		Document projectOutput = new Document( ) ;

		if ( logger.isDebugEnabled( ) ) {

			for ( String key : metricsId ) {

				projectOutput.append( key, 1 ) ;

			}

			projectOutput.append( "cpuCountAvg", 1 ) ;

		}

		projectOutput.append( CORES_USED, 1 ) ;
		projectOutput.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", 1 )
				.append( "_id", 0 ) ;

		if ( perVm ) {

			projectOutput.append( "host", "$_id.host" ) ;

		}

		Document projectOutputData = new Document( "$project", projectOutput ) ;
		operations.add( projectOutputData ) ;

		if ( StringUtils.isBlank( project ) ) {

			Document sortByProjectOrder = new Document( ) ;
			sortByProjectOrder.append( "project", 1 ) ;
			Document sortByProject = new Document( "$sort", sortByProjectOrder ) ;
			operations.add( sortByProject ) ;

		}

		logger.debug( "Query {}", operations ) ;
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( operations ) ;

		metricUtilities.stopTimer( timer, "vmCoreTrendingReport" ) ;
		return aggregationOutput ;

	}

	@Inject
	private MetricsDataHandler metricsDataHandler ;
	@Inject
	private ObjectMapper jsonMapper = new ObjectMapper( ) ;

	public JsonNode buildHostTrendReport (
											String appId ,
											String project ,
											String life ,
											String[] metricsId ,
											String[] divideBy ,
											String allVmTotal ,
											boolean reportHostUsage ,
											int top ,
											int low ,
											int numberOfDays ,
											int dateOffSet )
		throws Exception {

		logger.debug( "Metrics: {}, numDays: {} , Date: {}", Arrays.asList( metricsId ), numberOfDays, dateOffSet ) ;

		int analyticsDays = Math.abs( numberOfDays ) ;

		AggregateIterable<Document> aggregationOutput = buildHostDailyTrendReport(
				appId, project, life, metricsId,
				divideBy, allVmTotal,
				reportHostUsage,
				top, low, dateOffSet, analyticsDays ) ;

		var hostTrendReports = EventJsonConstants.transformToJackson( aggregationOutput ) ;

		if ( numberOfDays < 0 ) {

			buildHostHourlyTrendReport(
					appId, project, life,
					metricsId, divideBy, allVmTotal,
					reportHostUsage,
					top, low, dateOffSet,
					analyticsDays, hostTrendReports ) ;

		}

		return hostTrendReports ;

	}

	private void buildHostHourlyTrendReport (
												String appId ,
												String project ,
												String life ,
												String[] metricsId ,
												String[] divideBy ,
												String allVmTotal ,
												boolean reportHostUsage ,
												int top ,
												int low ,
												int dateOffSet ,
												int analyticsDays ,
												JsonNode hostTrendReports )
		throws IOException {

		var trendsWithHostReported = hostTrendReports ;

		if ( ! reportHostUsage ) {

			var perHostForReport = true ;
			AggregateIterable<Document> aggregationOutput = buildHostDailyTrendReport(
					appId, project, life, metricsId,
					divideBy, allVmTotal,
					perHostForReport,
					top, low, dateOffSet, analyticsDays ) ;
			trendsWithHostReported = EventJsonConstants.transformToJackson( aggregationOutput ) ;

		}

		//
		// hourly trends requested
		//

		var services = new String[0] ;

		logger.debug( "performing metrics query: {}, hostTrendReports: {}", reportHostUsage, trendsWithHostReported ) ;

		Map<String, JsonNode> hostMetricReports = CSAP.jsonStream( trendsWithHostReported )
				.map( hostTrendReport -> hostTrendReport.path( "host" ) )
				.filter( JsonNode::isTextual )
				.map( JsonNode::asText )
				.map( host -> {

					logger.debug( "metric report for host: {} ", host ) ;

					var hostMetricReport = jsonMapper.createObjectNode( ) ;
					hostMetricReport.put( "host", host ) ;

					try {

						var hostFullReport = jsonMapper.readTree( metricsDataHandler
								.buildMetricsReportNoCache(
										host, "host_30",
										"not-cached",
										analyticsDays, dateOffSet,
										0, 0,
										services, appId, life, false, false ) ) ;

						var metric = metricsId[0] ;

						if ( metric.equals( "totalCpuTestTime" ) ) {

							metric = "cpuTest" ;

						} else if ( metric.equals( "threadsTotal" ) ) {

							metric = "totalThreads" ;

						} else if ( metric.equals( "totalDiskTestTime" ) ) {

							metric = "diskTest" ;

						} else if ( metric.startsWith( "total" ) ) {

							metric = metric.substring( "total".length( ) ) ;
							metric = Character.toLowerCase( metric.charAt( 0 ) ) + metric.substring( 1 ) ;

						}

						hostMetricReport.set( "timeStamp",
								reduce_using_samples(
										hostFullReport.path( "data" ).path( "timeStamp" ),
										TREND_SAMPLES_FOR_24_HOURS,
										false, false ) ) ;

						hostMetricReport.set( metricsId[0],
								reduce_using_samples( hostFullReport.path( "data" ).path( metric ),
										TREND_SAMPLES_FOR_24_HOURS,
										true, false ) ) ;

					} catch ( Exception e ) {

						logger.warn( "Failed loading metrics: {} {}", host, CSAP.buildCsapStack( e ) ) ;

					}

					return hostMetricReport ;

				} )
				.collect( Collectors.toMap(
						hostMetricReport -> hostMetricReport.get( "host" ).asText( ),
						hostMetricReport -> hostMetricReport ) ) ;

		// logger.info( "hostMetricReports: {}", hostMetricReports ) ;

		//
		// trim down data
		//

		// hostMetricReports.values().stream()
		// .filter( JsonNode::isObject )
		// .map( hostData -> (ObjectNode) hostData )
		// .forEach( hostData -> {
		// hostData.set( "timeStamp", reduce_using_samples( hostData.path( "timeStamp"
		// ), HOST_TREND_SAMPLE_COUNT, false ) ) ;
		// hostData.set( metricsId[0], reduce_using_samples( hostData.path( metricsId[0]
		// ), HOST_TREND_SAMPLE_COUNT, true ) ) ;
		// } ) ;

		if ( reportHostUsage ) {

			// insert data directly from metrics
			CSAP.jsonStream( hostTrendReports )
					.filter( JsonNode::isObject )
					.map( hostTrendReport -> (ObjectNode) hostTrendReport )
					.forEach( hostTrendReport -> {

						var host = hostTrendReport.path( "host" ).asText( ) ;
						var hostMetricReport = hostMetricReports.get( host ) ;

						if ( hostMetricReport != null ) {

							hostTrendReport.set( "timeStamp", hostMetricReport.path( "timeStamp" ) ) ;
							hostTrendReport.set( metricsId[0], hostMetricReport.path( metricsId[0] ) ) ;

						} else {

							logger.warn( "no metrics for host: {}", host ) ;

						}

					} ) ;

		} else {

			// average all the data
			// CSAP.jsonStream( hostMetricReports ) ;
			var averagedData = jsonMapper.createArrayNode( ) ;
			var longestTimeStamp = jsonMapper.createArrayNode( ) ;
			hostMetricReports.values( ).stream( )
					.filter( JsonNode::isObject )
					.map( hostTrendReport -> (ObjectNode) hostTrendReport )
					.forEach( trendData -> {

						// timestamps
						var timeValues = trendData.path( "timeStamp" ) ;

						if ( timeValues.size( ) > longestTimeStamp.size( ) ) {

							longestTimeStamp.removeAll( ) ;
							longestTimeStamp.addAll( (ArrayNode) timeValues ) ;

						}

						var dataValues = trendData.path( metricsId[0] ) ;

						if ( averagedData.isEmpty( ) ) {

							averagedData.addAll( (ArrayNode) dataValues ) ;

						}

					} ) ;

			var firstEntry = (ObjectNode) hostTrendReports.path( 0 ) ;
			firstEntry.set( "timeStamp", longestTimeStamp ) ;
			firstEntry.set( metricsId[0], averagedData ) ;

		}

	}

	public JsonNode reduce_using_samples (
											JsonNode data ,
											int requestedSamples ,
											boolean calculateAverage ,
											boolean useTotal ) {

		var sampleInterval = Math.round( data.size( ) / requestedSamples ) ;

		if ( sampleInterval < 1 ) {

			sampleInterval = 1 ;

		}

		var trimmedData = jsonMapper.createArrayNode( ) ;
		double intervalAverageTotal = 0 ;
		int intervalTotalInt = 0 ;
		var intervalCount = 0 ;

		for ( var i = 0; i < data.size( ); i++ ) {

			boolean isFinalIteration = i == ( data.size( ) - 1 ) ;

			intervalCount++ ;

			var current = data.path( i ) ;

			if ( calculateAverage ) {

				intervalAverageTotal += current.asDouble( ) ;

			}

			if ( useTotal ) {

				intervalTotalInt += current.asInt( ) ;

			}

			if ( ( intervalCount % sampleInterval == 0 )
					|| isFinalIteration ) {

				if ( useTotal ) {

					trimmedData.add( intervalTotalInt ) ;

				} else if ( calculateAverage ) {

					trimmedData.add( CSAP.roundIt( intervalAverageTotal / intervalCount, 2 ) ) ;

				} else {

					trimmedData.add( current ) ;

				}

				intervalAverageTotal = 0 ;
				intervalTotalInt = 0 ;
				intervalCount = 0 ;

			}

		}

		return trimmedData ;

	}

	private AggregateIterable<Document> buildHostDailyTrendReport (
																	String appId ,
																	String project ,
																	String life ,
																	String[] metricsId ,
																	String[] divideBy ,
																	String allVmTotal ,
																	boolean perVm ,
																	int top ,
																	int low ,
																	int dateOffSet ,
																	int analyticsDays ) {

		var timer = metricUtilities.startTimer( ) ;
		String category = "/csap/reports/host/daily" ;
		List<Document> trendingCommandPipleline = new ArrayList<>( ) ;

		Set matchHosts = new HashSet( ) ;

		if ( top > 0 ) {

			List topHosts = trendingReportHelper.topHosts( appId, project, life, metricsId,
					new String[] {
							"numberOfSamples"
					}, top, -1,
					category, "", false, analyticsDays, dateOffSet ) ;
			matchHosts.addAll( topHosts ) ;

		}

		if ( low > 0 ) {

			List lowHosts = trendingReportHelper.topHosts( appId, project, life, metricsId,
					new String[] {
							"numberOfSamples"
					}, low, 1,
					category, "", false, analyticsDays, dateOffSet ) ;
			matchHosts.addAll( lowHosts ) ;

		}

		Document query = constructDocumentQuery( appId, project, life, category ) ;
		query = addDateToDocumentQuery( query, analyticsDays, dateOffSet ) ;

		if ( matchHosts.size( ) > 0 ) {

			Document hostMatch = new Document( "$in", matchHosts ) ;
			query.append( "host", hostMatch ) ;

		}

		Document match = new Document( "$match", query ) ;
		trendingCommandPipleline.add( match ) ;

		if ( "true".equalsIgnoreCase( allVmTotal ) || perVm ) {

			Document groupByHost = trendingReportHelper.groupByHostNameDocument( metricsId, divideBy ) ;
			trendingCommandPipleline.add( groupByHost ) ;
			trendingReportHelper.addHostDivideByProjectionsToPipeline( metricsId, divideBy, trendingCommandPipleline ) ;

			if ( ! perVm ) {

				Document groupByLife = trendingReportHelper.groupByLife( metricsId ) ;
				trendingCommandPipleline.add( groupByLife ) ;

			}

		} else {

			Map<String, Object> groupFieldMap = new HashMap<>( ) ;
			groupFieldMap.put( "appId", "$appId" ) ;
			groupFieldMap.put( "project", "$project" ) ;
			groupFieldMap.put( "lifecycle", "$lifecycle" ) ;
			groupFieldMap.put( "date", "$createdOn.date" ) ;
			Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

			for ( String key : metricsId ) {

				groupFields.append( key, new Document( "$sum", "$data.summary." + key ) ) ;

			}

			if ( ArrayUtils.contains( divideBy, "numberOfSamples" ) ) {

				groupFields.append( "numberOfSamples", new BasicDBObject( "$sum", "$data.summary.numberOfSamples" ) ) ;

			}

			Document group = new Document( "$group", groupFields ) ;
			trendingCommandPipleline.add( group ) ;

		}

		Document projectFields = new Document( ) ;
		List<Object> metricsList = new ArrayList<>( ) ;

		for ( String key : metricsId ) {

			metricsList.add( "$" + key ) ;
			projectFields.append( key, 1 ) ;

		}

		if ( metricsId.length > 1 ) {

			projectFields.append( "total", new BasicDBObject( "$add", metricsList ) ) ;

		}

		projectFields.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", "$_id.date" )
				.append( "_id", 0 ) ;

		if ( perVm ) {

			projectFields.append( "host", "$_id.host" ) ;

		}

		if ( ArrayUtils.contains( divideBy, "numberOfSamples" ) ) {

			projectFields.append( "numberOfSamples", 1 ) ;

		}

		Document projectData = new Document( "$project", projectFields ) ;
		trendingCommandPipleline.add( projectData ) ;

		if ( null != divideBy && ! "true".equalsIgnoreCase( allVmTotal ) && ! perVm ) {

			String dividend = metricsId[0] ;

			if ( metricsId.length > 1 ) {

				dividend = "total" ;

			}

			logger.debug( "dividend {} ", dividend ) ;

			for ( String divisorStr : divideBy ) {

				Document numSampleProjection = new Document( ) ;
				List<Object> numSampleDivideList = new ArrayList<>( ) ;
				numSampleDivideList.add( "$" + dividend ) ;

				if ( divisorStr.equalsIgnoreCase( "numberOfSamples" ) ) {

					numSampleDivideList.add( "$numberOfSamples" ) ;

				} else {

					if ( NumberUtils.isNumber( divisorStr ) ) {

						numSampleDivideList.add( Double.parseDouble( divisorStr ) ) ;

					}

				}

				numSampleProjection.append( "appId", 1 )
						.append( "lifecycle", 1 )
						.append( "project", 1 )
						.append( "numberOfSamples", 1 )
						.append( "date", 1 ) ;
				numSampleProjection.append( dividend, new BasicDBObject( "$divide", numSampleDivideList ) ) ;
				Document projectDivision = new Document( "$project", numSampleProjection ) ;

				if ( numSampleDivideList.size( ) == 2 ) {

					trendingCommandPipleline.add( projectDivision ) ;

				}

			}

		}

		Document sortOrder = new Document( ) ;
		sortOrder.append( "date", 1 ) ;
		Document sort = new Document( "$sort", sortOrder ) ;
		trendingCommandPipleline.add( sort ) ;

		Map<String, Object> groupByDateMap = new HashMap<>( ) ;
		groupByDateMap.put( "appId", "$appId" ) ;
		groupByDateMap.put( "project", "$project" ) ;
		groupByDateMap.put( "lifecycle", "$lifecycle" ) ;

		if ( perVm ) {

			groupByDateMap.put( "host", "$host" ) ;

		}

		Document groupVmData = new Document( "_id", new Document( groupByDateMap ) ) ;
		groupVmData.append( "date", new Document( "$push", "$date" ) ) ;

		for ( String key : metricsId ) {

			groupVmData.append( key, new Document( "$push", "$" + key ) ) ;

		}

		if ( metricsId.length > 1 ) {

			groupVmData.append( "total", new Document( "$push", "$total" ) ) ;

		}

		Document groupData = new Document( "$group", groupVmData ) ;
		trendingCommandPipleline.add( groupData ) ;

		Document projectOutput = new Document( ) ;

		if ( metricsId.length > 1 ) {

			projectOutput.append( "total", 1 ) ;

			if ( logger.isDebugEnabled( ) ) {

				for ( String key : metricsId ) {

					projectOutput.append( key, 1 ) ;

				}

			}

		} else {

			for ( String key : metricsId ) {

				projectOutput.append( key, 1 ) ;

			}

		}

		projectOutput.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", 1 )
				.append( "_id", 0 ) ;

		if ( perVm ) {

			projectOutput.append( "host", "$_id.host" ) ;

		}

		Document projectOutputData = new Document( "$project", projectOutput ) ;
		trendingCommandPipleline.add( projectOutputData ) ;

		if ( StringUtils.isBlank( project ) ) {

			Document sortByProjectOrder = new Document( ) ;
			sortByProjectOrder.append( "project", 1 ) ;
			Document sortByProject = new Document( "$sort", sortByProjectOrder ) ;
			trendingCommandPipleline.add( sortByProject ) ;

		}

		logger.debug( "trendingCommandPipleline: {} ", trendingCommandPipleline ) ;

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( trendingCommandPipleline ) ;

		metricUtilities.stopTimer( timer, "vmTrending" ) ;
		return aggregationOutput ;

	}

	public AggregateIterable<Document> getVmReport (
														String appId ,
														String project ,
														String life ,
														int numDays ,
														int dateOffSet ) {

		String category = "/csap/reports/host/daily" ;
		Document query = constructDocumentQuery( appId, project, life, category ) ;
		query = addDateToDocumentQuery( query, numDays, dateOffSet ) ;
		Document match = new Document( "$match", query ) ;

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( APP_ID, "$" + APP_ID ) ;
		groupFieldMap.put( PROJECT, "$" + PROJECT ) ;
		groupFieldMap.put( LIFE_CYCLE, "$" + LIFE_CYCLE ) ;
		groupFieldMap.put( HOST, "$" + HOST ) ;

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

		Document projectFields = new Document( HOST_NAME, "$_id." + HOST ) ;
		projectFields.append( APP_ID, "$_id." + APP_ID ).append( LIFE_CYCLE, "$_id." + LIFE_CYCLE )
				.append( PROJECT, "$_id." + PROJECT ).append( "_id", 0 ) ;
		Set<String> keys = analyticsHelper.findReportDocumentAttributes( appId, project, life, category, null ) ;
		if ( null == keys )
			keys = new HashSet<>( ) ;

		for ( String key : keys ) {

			if ( key.endsWith( "Avg" ) ) {

				groupFields.append( key, new Document( "$avg", "$data.summary." + key ) ) ;

			} else {

				groupFields.append( key, new Document( "$sum", "$data.summary." + key ) ) ;

			}

			projectFields.append( key, 1 ) ;

		}

		Document group = new Document( "$group", groupFields ) ;
		Document projectHost = new Document( "$project", projectFields ) ;

		List<Document> operations = new ArrayList<>( ) ;
		operations.add( match ) ;
		operations.add( group ) ;
		operations.add( projectHost ) ;
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( operations ) ;
		return aggregationOutput ;

	}

	public Document constructDocumentQuery ( String appId , String project , String life , String category ) {

		Document query = new Document( ) ;

		if ( StringUtils.isNotBlank( appId ) ) {

			query.append( APP_ID, appId ) ;

		}

		if ( StringUtils.isNotBlank( project ) ) {

			query.append( PROJECT, project ) ;

		}

		if ( StringUtils.isNotBlank( life ) ) {

			query.append( LIFE_CYCLE, life ) ;

		}

		if ( StringUtils.isNotBlank( category ) ) {

			query.append( CATEGORY, category ) ;

		}

		return query ;

	}

	private Document addDateToDocumentQuery ( Document query , int numDays , int dateOffSet ) {

		return trendingReportHelper.addDateToDocumentQuery( query, numDays, dateOffSet ) ;

	}

	final static public int MONGO_ASCENDING_SORT = 1 ;

	@Cacheable ( value = CsapEventsApplication.NUM_DAYS_CACHE )
	public long numDaysAnalyticsAvailable (
											String appId ,
											String project ,
											String life ,
											String reportType ,
											String serviceName ) {

		var timer = metricUtilities.startTimer( ) ;

		long numDaysAvailable = 0 ;
		Document query = buildDayCountQuery( appId, project, life, reportType, serviceName ) ;
		Document sortOrder = new Document( "createdOn.date", MONGO_ASCENDING_SORT ) ;

		Document earliestRecord = analyticsHelper.getMongoEventCollection( )
				.find( query )
				.sort( sortOrder )
				.limit( 1 )
				.projection( fields( include( "createdOn" ), excludeId( ) ) )
				.first( ) ;

		if ( null != earliestRecord ) {

			Document createdOnDbObject = (Document) earliestRecord.get( CREATED_ON ) ;
			Date earliestDate = (Date) createdOnDbObject.get( "mongoDate" ) ;
			logger.debug( "Earliest Date ::--> {}", earliestDate ) ;
			long diff = Calendar.getInstance( ).getTimeInMillis( ) - earliestDate.getTime( ) ;
			numDaysAvailable = TimeUnit.DAYS.convert( diff, TimeUnit.MILLISECONDS ) ;

		}

		var nanos = metricUtilities.stopTimer( timer, "reportsNumDays" ) ;

		logger.debug( "Days Of Data: {}. Time Taken: {} \n Report: {}",
				numDaysAvailable,
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ),
				query ) ;

		return numDaysAvailable ;

	}

	final static String SLASH_DETAILS = "/details" ;

	private Document buildDayCountQuery (
											String appId ,
											String project ,
											String life ,
											String reportType ,
											String serviceName ) {

		Document query = new Document( ) ;
		query.append( APP_ID, appId )
				.append( PROJECT, project )
				.append( LIFE_CYCLE, life ) ;

		logger.debug( "reportType: {}", reportType ) ;

		switch ( reportType ) {

		case "userreport":
			Pattern uiPattern = Pattern.compile( "^/csap/ui/" ) ;
			query.append( CATEGORY, uiPattern ) ;
			break ;

		case "logRotateReport":
			query.append( CATEGORY, "/csap/reports/logRotate" ) ;
			break ;

		case "hostreport":
			query.append( CATEGORY, "/csap/reports/host/daily" ) ;
			break ;

		default:
			query.append( CATEGORY, reportType ) ;
			break ;

		}

		if ( StringUtils.isNotBlank( serviceName ) ) {

			Document serviceNameObj = new Document( "serviceName", serviceName ) ;
			query.append( "data.summary", new Document( "$elemMatch", serviceNameObj ) ) ;

		}

		return query ;

	}

	private Document getGroupByAppIdProjectLifeAndDate ( ) {

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( APP_ID, "$" + APP_ID ) ;
		groupFieldMap.put( PROJECT, "$" + PROJECT ) ;
		groupFieldMap.put( LIFE_CYCLE, "$" + LIFE_CYCLE ) ;
		groupFieldMap.put( DATE, "$" + CREATED_ON + "." + DATE ) ;
		Document groupFields = new Document( "_id", new BasicDBObject( groupFieldMap ) ) ;
		Document group = new Document( "$group", groupFields ) ;
		return group ;

	}

	private Document getGroupByAppIdProjectLife ( ) {

		Map<String, Object> groupFieldsMap = new HashMap<>( ) ;
		groupFieldsMap.put( APP_ID, "$_id." + APP_ID ) ;
		groupFieldsMap.put( PROJECT, "$_id." + PROJECT ) ;
		groupFieldsMap.put( LIFE_CYCLE, "$_id." + LIFE_CYCLE ) ;
		Document projectGroupFields = new Document( "_id", new BasicDBObject( groupFieldsMap ) ) ;
		projectGroupFields.put( "totNumDays", new Document( "$sum", 1 ) ) ;
		Document group = new Document( "$group", projectGroupFields ) ;
		return group ;

	}

}
