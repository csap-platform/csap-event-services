package org.csap.events.db ;

import static org.csap.events.util.MetricsJsonConstants.APP_ID ;
import static org.csap.events.util.MetricsJsonConstants.CATEGORY ;
import static org.csap.events.util.MetricsJsonConstants.CREATED_ON ;
import static org.csap.events.util.MetricsJsonConstants.DATE ;
import static org.csap.events.util.MetricsJsonConstants.LIFE_CYCLE ;
import static org.csap.events.util.MetricsJsonConstants.PROJECT ;
import static org.csap.events.util.MongoConstants.EVENT_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.EVENT_DB_NAME ;

import java.util.ArrayList ;
import java.util.HashMap ;
import java.util.List ;
import java.util.Map ;
import java.util.stream.Collectors ;

import javax.inject.Inject ;

import org.apache.commons.lang3.ArrayUtils ;
import org.apache.commons.lang3.StringUtils ;
import org.apache.commons.lang3.math.NumberUtils ;
import org.bson.Document ;
import org.csap.events.EventJsonConstants ;
import org.csap.events.util.DateUtil ;
import org.csap.helpers.CSAP ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.fasterxml.jackson.databind.ObjectMapper ;
import com.mongodb.BasicDBObject ;
import com.mongodb.DBCollection ;
import com.mongodb.MongoClient ;
import com.mongodb.client.AggregateIterable ;
import com.mongodb.client.MongoCursor ;

public class TrendingReportHelper {

	public static final String MULTIPLE_SERVICE_DELIMETER = "," ;
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Inject
	private MongoClient mongoClient ;

	@Inject
	private AnalyticsHelper analyticsHelper ;

	public List topHosts (
							String appId ,
							String project ,
							String life ,
							String[] metricsId ,
							String[] divideBy ,
							int numHosts ,
							int sortOrd ,
							String category ,
							String serviceNameFilter ,
							boolean unwindSummary ,
							int numDays ,
							int dateOffSet ) {

		if ( logger.isDebugEnabled( ) ) {

			for ( String key : metricsId ) {

				logger.debug( "Metrics ID {} ", key ) ;

			}

			logger.debug( "Service name filter {} ", serviceNameFilter ) ;
			logger.debug( "Unwind summary {} ", unwindSummary ) ;

		}

		String metricsPath = getReportPath( category ) ;
		logger.debug( "metrics path {} ", metricsPath ) ;
		List<Document> aggregationPipeline = new ArrayList<>( ) ;
		Document query = constructDocumentQuery( appId, project, life, category ) ;
		query = addDateToDocumentQuery( query, numDays, dateOffSet ) ;
		Document match = new Document( "$match", query ) ;
		aggregationPipeline.add( match ) ;

		if ( unwindSummary ) {

			Document unwind = new Document( "$unwind", "$" + metricsPath ) ;
			aggregationPipeline.add( unwind ) ;

			if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

				Document filterService = new Document( metricsPath + ".serviceName", serviceNameFilter ) ;
				Document serviceMatch = new Document( "$match", filterService ) ;
				aggregationPipeline.add( serviceMatch ) ;

			}

		}

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( "appId", "$appId" ) ;

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			groupFieldMap.put( "serviceName", "$" + metricsPath + ".serviceName" ) ;

		}

		if ( StringUtils.isNotBlank( project ) ) {

			groupFieldMap.put( "project", "$project" ) ;

		}

		if ( StringUtils.isNotBlank( life ) ) {

			groupFieldMap.put( "lifecycle", "$lifecycle" ) ;

		}

		groupFieldMap.put( "host", "$host" ) ;

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

		for ( String metricsKey : metricsId ) {

			groupFields.append( metricsKey, new Document( "$sum", "$" + metricsPath + "." + metricsKey ) ) ;

		}

		groupFields.append( "numberOfSamples", new Document( "$sum", "$" + metricsPath + ".numberOfSamples" ) ) ;
		Document group = new Document( "$group", groupFields ) ;
		aggregationPipeline.add( group ) ;

		Document projectFields = new Document( ) ;
		List<Object> metricsList = new ArrayList<>( ) ;

		for ( String metricsKey : metricsId ) {

			metricsList.add( "$" + metricsKey ) ;

		}

		projectFields.append( "total", new Document( "$add", metricsList ) ) ;
		projectFields
				.append( "appId", "$_id.appId" )
				.append( "host", "$_id.host" )
				.append( "numberOfSamples", 1 )
				.append( "_id", 0 ) ;

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			projectFields.append( "serviceName", "$_id.serviceName" ) ;

		}

		if ( StringUtils.isNotBlank( project ) ) {

			projectFields.append( "project", "$_id.project" ) ;

		}

		if ( StringUtils.isNotBlank( life ) ) {

			projectFields.append( "lifecycle", "$_id.lifecycle" ) ;

		}

		Document projectData = new Document( "$project", projectFields ) ;
		aggregationPipeline.add( projectData ) ;

		addHostDivideByProjectionsToPipeline( new String[] {
				"total"
		}, divideBy, aggregationPipeline ) ;

		Document sortOrder = new Document( ) ;
		// -1 is highest first
		// 1 lowest first
		sortOrder.append( "total", sortOrd ) ;
		Document sort = new Document( "$sort", sortOrder ) ;
		aggregationPipeline.add( sort ) ;

		Map<String, Object> groupByDateMap = new HashMap<>( ) ;
		groupByDateMap.put( "appId", "$appId" ) ;

		if ( StringUtils.isNotBlank( project ) ) {

			groupByDateMap.put( "project", "$project" ) ;

		}

		if ( StringUtils.isNotBlank( life ) ) {

			groupByDateMap.put( "lifecycle", "$lifecycle" ) ;

		}

		Document groupVmData = new Document( "_id", new Document( groupByDateMap ) ) ;
		groupVmData.append( "host", new Document( "$push", "$host" ) ) ;
		groupVmData.append( "total", new Document( "$push", "$total" ) ) ;
		Document groupData = new Document( "$group", groupVmData ) ;
		aggregationPipeline.add( groupData ) ;

		// DBCollection eventCollection = getEventCollection();
		// AggregationOutput output =
		// eventCollection.aggregate(aggregationPipeline,ReadPreference.secondaryPreferred());

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection( )
				.aggregate( aggregationPipeline ) ;
		// logger.debug("query {} ",output.getCommand());
		List topHosts = new ArrayList( ) ;
		MongoCursor<Document> cursor = aggregationOutput.iterator( ) ;

		while ( cursor.hasNext( ) ) {

			Document hostDocument = cursor.next( ) ;
			logger.debug( "hostDocument {} ", hostDocument ) ;
			List dbHostList = (List) hostDocument.get( "host" ) ;

			if ( null != dbHostList ) {

				int toIndex = numHosts ;

				if ( dbHostList.size( ) < numHosts || numHosts == 0 ) {

					toIndex = dbHostList.size( ) ;

				}

				topHosts.addAll( dbHostList.subList( 0, toIndex ) ) ;

			}

		}

		/*
		 * output.results().forEach(dbObject -> { logger.debug("dbObject {} ",dbObject);
		 * BasicDBList dbHostList = (BasicDBList) dbObject.get("host"); if(null !=
		 * dbHostList){ int toIndex = numHosts; if(dbHostList.size() < numHosts ||
		 * numHosts == 0){ toIndex = dbHostList.size(); }
		 * //logger.debug("To Index {} ",toIndex); topHosts.addAll(dbHostList.subList(0,
		 * toIndex)); } });
		 */
		return topHosts ;

	}

	private Document buildGroupByService ( String[] metricsId , String[] divideBy , boolean isIncludeService ) {

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;

		if ( isIncludeService ) {

			groupFieldMap.put( "serviceName", "$data.summary.serviceName" ) ;

		}

		groupFieldMap.put( "appId", "$appId" ) ;
		groupFieldMap.put( "project", "$project" ) ;
		groupFieldMap.put( "lifecycle", "$lifecycle" ) ;
		groupFieldMap.put( "date", "$createdOn.date" ) ;
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

		for ( String key : metricsId ) {

			groupFields.append( key, new Document( "$sum", "$data.summary." + key ) ) ;

		}

		if ( ArrayUtils.contains( divideBy, "numberOfSamples" ) ) {

			groupFields.append( "numberOfSamples", new Document( "$sum", "$data.summary.numberOfSamples" ) ) ;

		}

		Document group = new Document( "$group", groupFields ) ;
		return group ;

	}

	public Document groupByHostNameDocument ( String[] metricsId , String[] divideBy ) {

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( "appId", "$appId" ) ;
		groupFieldMap.put( "project", "$project" ) ;
		groupFieldMap.put( "lifecycle", "$lifecycle" ) ;
		groupFieldMap.put( "date", "$createdOn.date" ) ;
		groupFieldMap.put( "host", "$host" ) ;
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

		for ( String key : metricsId ) {

			groupFields.append( key, new Document( "$sum", "$data.summary." + key ) ) ;

		}

		groupFields.append( "numberOfSamples", new Document( "$sum", "$data.summary.numberOfSamples" ) ) ;

		Document group = new Document( "$group", groupFields ) ;
		return group ;

	}

	private Document reportTrendEnsureFieldsProjection ( String[] metricsId ) {

		Document projectCondition = new Document( ) ;
		projectCondition.append( "appId", 1 )
				.append( "lifecycle", 1 )
				.append( "project", 1 )
				.append( "serviceName", 1 )
				// .append("numberOfSamples", 1)
				.append( "date", 1 ) ;

		for ( String key : metricsId ) {

			projectCondition.append( key, 1 ) ;

		}

		if ( metricsId.length > 1 ) {

			projectCondition.append( "total", 1 ) ;

		}

		//
		// Handle option sample count
		//
		var sampleCountEquals = new ArrayList<>( ) ;
		sampleCountEquals.add( "$numberOfSamples" ) ;
		sampleCountEquals.add( Integer.toString( 0 ) ) ;

		var sampleCountMath = new ArrayList<>( ) ;
		sampleCountMath.add( new Document( "$eq", sampleCountEquals ) ) ;
		sampleCountMath.add( Integer.toString( 1 ) ) ;
		sampleCountMath.add( "$numberOfSamples" ) ;
		Document sampleCountExpression = new Document( "$cond", sampleCountMath ) ;
		projectCondition.append( "numberOfSamples", sampleCountExpression ) ;

		//
		// Handle new containerCount Attribute
		//
		var containerCountEquals = new ArrayList<>( ) ;
		containerCountEquals.add( "$countCsapMean" ) ;
		containerCountEquals.add( 0 ) ;

		var containerCountMath = new ArrayList<>( ) ;
		containerCountMath.add( new Document( "$eq", containerCountEquals ) ) ;
		containerCountMath.add( 1 ) ;
		containerCountMath.add( "$countCsapMean" ) ;
		Document containerCountExpression = new Document( "$cond", containerCountMath ) ;
		projectCondition.append( "countCsapMean", containerCountExpression ) ;

		Document projCondition = new Document( "$project", projectCondition ) ;

		// operations.add(projCondition);
		return projCondition ;

	}

	private Document groupByServiceNameAndHostName ( String[] metricsId , String[] divideBy ) {

		var groupFieldMap = new HashMap<String, Object>( ) ;

		groupFieldMap.put( "serviceName", "$data.summary.serviceName" ) ;

		groupFieldMap.put( "appId", "$appId" ) ;
		groupFieldMap.put( "project", "$project" ) ;
		groupFieldMap.put( "lifecycle", "$lifecycle" ) ;
		groupFieldMap.put( "host", "$host" ) ;
		groupFieldMap.put( "date", "$createdOn.date" ) ;

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

		for ( var key : metricsId ) {

			groupFields.append( key, new Document( "$sum", "$data.summary." + key ) ) ;

		}

		groupFields.append( "numberOfSamples", new Document( "$sum", "$data.summary.numberOfSamples" ) ) ;

		// multiple container support
		groupFields.append( "countCsapMean", new Document( "$sum", "$data.summary.countCsapMean" ) ) ;

		Document group = new Document( "$group", groupFields ) ;
		return group ;

	}

	private void addMetricCalculationProjection (
													String[] metricsIds ,
													String[] divideBys ,
													List<Document> aggregationPipeline ) {

		if ( null == divideBys ) {

			return ;

		}

		for ( var metricId : metricsIds ) {

			for ( var divideBy : divideBys ) {

				Document calculationProjection = new Document( ) ;

				calculationProjection.append( "appId", EventJsonConstants.INCLUDE_FIELD )
						.append( "project", EventJsonConstants.INCLUDE_FIELD )
						.append( "lifecycle", EventJsonConstants.INCLUDE_FIELD )
						.append( "host", EventJsonConstants.INCLUDE_FIELD )
						.append( "numberOfSamples", EventJsonConstants.INCLUDE_FIELD )
						.append( "date", EventJsonConstants.INCLUDE_FIELD ) ;

				for ( String metricsIdForProjection : metricsIds ) {

					if ( ! metricId.equalsIgnoreCase( metricsIdForProjection ) ) {

						calculationProjection.append( metricsIdForProjection, EventJsonConstants.INCLUDE_FIELD ) ;

					}

				}

//	            "threadCountTotal" : {
//	                "$divide" : [ 
//	                    { "$multiply" : [ "$threadCount", "$countCsapMean" ] }, 
//	                    "$numberOfSamples" 
//	                ]
//	            }

				var divideItems = new ArrayList<>( ) ;

				var metricField = "$" + metricId ;

				if ( divideBy.equalsIgnoreCase( "numberOfSamples" ) ) {

					// divideItems.add( "$" + metricId ) ;
					var multiplyBy = new Document( "$multiply", List.of( metricField, "$countCsapMean" ) ) ;
					divideItems.add( multiplyBy ) ;
					divideItems.add( "$numberOfSamples" ) ;

				} else {

					divideItems.add( metricField ) ;

					if ( NumberUtils.isNumber( divideBy ) ) {

						divideItems.add( Double.parseDouble( divideBy ) ) ;

					}

				}

				calculationProjection.append( metricId, new Document( "$divide", divideItems ) ) ;

				if ( divideItems.size( ) == 2 ) {

					aggregationPipeline.add(
							new Document( "$project", calculationProjection ) ) ;

				}

			}

		}

	}

	public void addHostDivideByProjectionsToPipeline (
														String[] metricsId ,
														String[] divideBy ,
														List<Document> aggregationPipeline ) {

		if ( null != divideBy ) {

			for ( String key : metricsId ) {

				for ( String divisorStr : divideBy ) {

					Document numSampleProjection = new Document( ) ;
					List<Object> numSampleDivideList = new ArrayList<>( ) ;
					numSampleDivideList.add( "$" + key ) ;

					if ( divisorStr.equalsIgnoreCase( "numberOfSamples" ) ) {

						numSampleDivideList.add( "$numberOfSamples" ) ;

					} else {

						if ( NumberUtils.isNumber( divisorStr ) ) {

							numSampleDivideList.add( Double.parseDouble( divisorStr ) ) ;

						}

					}

					for ( String metricsIdForProjection : metricsId ) {

						if ( ! key.equalsIgnoreCase( metricsIdForProjection ) ) {

							numSampleProjection.append( metricsIdForProjection, 1 ) ;

						}

					}

					numSampleProjection.append( "appId", 1 )
							.append( "project", 1 )
							.append( "lifecycle", 1 )
							.append( "host", 1 )
							.append( "numberOfSamples", 1 )
							.append( "date", 1 ) ;
					numSampleProjection.append( key, new Document( "$divide", numSampleDivideList ) ) ;
					Document projectDivision = new Document( "$project", numSampleProjection ) ;

					if ( numSampleDivideList.size( ) == 2 ) {

						aggregationPipeline.add( projectDivision ) ;

					}

				}

			}

		}

	}

	public Document groupByLife ( String[] metricsId ) {

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;
		groupFieldMap.put( "appId", "$_id.appId" ) ;
		groupFieldMap.put( "project", "$_id.project" ) ;
		groupFieldMap.put( "lifecycle", "$_id.lifecycle" ) ;
		groupFieldMap.put( "date", "$_id.date" ) ;
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

		for ( String key : metricsId ) {

			groupFields.append( key, new Document( "$sum", "$" + key ) ) ;

		}

		groupFields.append( "numberOfSamples", new Document( "$sum", "$numberOfSamples" ) ) ;
		Document group = new Document( "$group", groupFields ) ;
		return group ;

	}

	private Document buildPrimaryProjectionGrouping ( String[] metricsId ) {

		Map<String, Object> groupFieldMap = new HashMap<>( ) ;

		groupFieldMap.put( "serviceName", "$_id.serviceName" ) ;
		groupFieldMap.put( "appId", "$_id.appId" ) ;
		groupFieldMap.put( "project", "$_id.project" ) ;
		groupFieldMap.put( "lifecycle", "$_id.lifecycle" ) ;
		groupFieldMap.put( "date", "$_id.date" ) ;
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) ) ;

		for ( String key : metricsId ) {

			groupFields.append( key, new Document( "$sum", "$" + key ) ) ;

		}

		groupFields.append( "numberOfSamples", new Document( "$sum", "$numberOfSamples" ) ) ;
		Document group = new Document( "$group", groupFields ) ;
		return group ;

	}

	public Document addDateToDocumentQuery ( Document query , int numDays , int dateOffSet ) {

		int startOffSet = dateOffSet + numDays ;
		int endOffSet = dateOffSet ;
		String fromDate = DateUtil.buildMongoCreatedDateFromOffset( startOffSet ) ;
		String toDate = DateUtil.buildMongoCreatedDateFromOffset( endOffSet ) ;
		query.append( CREATED_ON + "." + DATE, new Document( "$gt", fromDate ).append( "$lte", toDate ) ) ;
		logger.debug( "Query: {} ", query.toString( ) ) ;
		return query ;

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

	private String getReportPath ( String category ) {

		if ( "/csap/reports/health".equalsIgnoreCase( category ) ) {

			return "data" ;

		} else {

			return "data.summary" ;

		}

	}

	private DBCollection getEventCollection ( ) {

		return mongoClient.getDB( EVENT_DB_NAME ).getCollection( EVENT_COLLECTION_NAME ) ;

	}

	public List<Document> trendingOperationPipelineBuilder (
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
																int dateOffSet ) {

		List<Document> mongoOperationPipeline = new ArrayList<>( ) ;

		mongoOperationPipeline.add(
				trendingPrimaryQueryBuilder( appId, project, life, serviceNameFilter, category,
						numDays, dateOffSet ) ) ;

		mongoOperationPipeline.add( new Document( "$unwind", "$data.summary" ) ) ;

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			List<Document> serviceList = new ArrayList<Document>( ) ;

			for ( String name : serviceNameFilter.split( MULTIPLE_SERVICE_DELIMETER ) ) {

				serviceList.add( new Document( "data.summary.serviceName", name ) ) ;

			}

			Document unwindMatchOp = new Document( "$or", serviceList ) ;
			mongoOperationPipeline.add( new Document( "$match", unwindMatchOp ) ) ;

		}

		if ( "true".equalsIgnoreCase( allVmTotal ) ) {

			logger.debug( "allVmTotal true: total all hosts together" ) ;

			mongoOperationPipeline.add(
					groupByServiceNameAndHostName( metricsId, divideBy ) ) ;

			mongoOperationPipeline.add(
					reportTrendEnsureFieldsProjection( metricsId ) ) ;

			addMetricCalculationProjection(
					metricsId,
					divideBy,
					mongoOperationPipeline ) ;
			// group by service name
			Document op4_sum_the_host_values = buildPrimaryProjectionGrouping( metricsId ) ;
			mongoOperationPipeline.add( op4_sum_the_host_values ) ;

		} else {

			logger.debug( "allVmTotal false: average across all collecte hosts" ) ;

			boolean isIncludeService = true ;
			if ( serviceNameFilter != null && serviceNameFilter.contains( MULTIPLE_SERVICE_DELIMETER ) )
				isIncludeService = false ;

			if ( byHost ) {

				logger.debug( "grouping by host" ) ;
				Document groupByHost = groupByHostNameDocument( metricsId, divideBy ) ;
				mongoOperationPipeline.add( groupByHost ) ;

			} else {

				mongoOperationPipeline.add(
						buildGroupByService( metricsId, divideBy, isIncludeService ) ) ;

			}

		}

		mongoOperationPipeline.add(
				trendingProjectionBuilder( byHost, metricsId, divideBy ) ) ;

		mongoOperationPipeline.add(
				trendingProjectionConditionBuilder( byHost, metricsId ) ) ;

		if ( null != divideBy && ! "true".equalsIgnoreCase( allVmTotal ) ) {

			trendingDivideByBuilder( byHost, metricsId, divideBy, mongoOperationPipeline ) ;

		}

		if ( StringUtils.isBlank( serviceNameFilter ) ) {

			mongoOperationPipeline.add( trendingGroupByLifeBuilder( metricsId ) ) ;
			mongoOperationPipeline.add( trendingProjectionByLifeBuilder( metricsId ) ) ;

		}

		Document sortOrder = new Document( ) ;
		sortOrder.append( "date", 1 ) ;
		mongoOperationPipeline.add( new Document( "$sort", sortOrder ) ) ;

		mongoOperationPipeline.add(
				trendingOutputGroupingBuilder( byHost, serviceNameFilter, metricsId ) ) ;

		mongoOperationPipeline.add(
				trendingOutputFieldSelectionBuilder( byHost, serviceNameFilter, metricsId ) ) ;

		if ( StringUtils.isBlank( project ) ) {

			Document sortByProjectOrder = new Document( ) ;
			sortByProjectOrder.append( "project", 1 ) ;
			Document sortByProject = new Document( "$sort", sortByProjectOrder ) ;
			mongoOperationPipeline.add( sortByProject ) ;

		}

		if ( logger.isDebugEnabled( ) ) {

			var formattedPipeLine = mongoOperationPipeline.stream( )
					.map( doc -> {

						var json = "" ;

						try {

							json = CSAP.jsonPrint( jsonMapper.readTree( doc.toJson( ) ) ) ;

						} catch ( Exception e ) {

							logger.info( "Failed debug output: {} ", CSAP.buildCsapStack( e ) ) ;

						}

						return json ;

					} )
					.collect( Collectors.joining( "\n\t" ) ) ;

			logger.debug( "formattedPipeLine {} ", formattedPipeLine ) ;

		}

		return mongoOperationPipeline ;

	}

	@Inject
	private ObjectMapper jsonMapper = new ObjectMapper( ) ;

	private Document trendingOutputFieldSelectionBuilder (
															boolean byHost ,
															String serviceNameFilter ,
															String[] metricsId ) {

		Document primaryTrendingProjection = new Document( ) ;

		for ( String key : metricsId ) {

			primaryTrendingProjection.append( key, 1 ) ;

		}

		if ( metricsId.length > 1 ) {

			primaryTrendingProjection.append( "total", 1 ) ;

			if ( logger.isDebugEnabled( ) ) {

				for ( String key : metricsId ) {

					primaryTrendingProjection.append( key, 1 ) ;

				}

			}

		} else {

			for ( String key : metricsId ) {

				primaryTrendingProjection.append( key, 1 ) ;

			}

		}

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			primaryTrendingProjection.append( "serviceName", "$_id.serviceName" ) ;

		}

		primaryTrendingProjection
				.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )

				.append( "project", "$_id.project" )
				.append( "date", 1 )
				.append( "_id", 0 ) ;

		if ( byHost ) {

			primaryTrendingProjection.append( "host", "$_id.host" ) ;

		}

		Document primaryTrendingProjectionOp = new Document( "$project", primaryTrendingProjection ) ;
		return primaryTrendingProjectionOp ;

	}

	private Document trendingOutputGroupingBuilder ( boolean byHost , String serviceNameFilter , String[] metricsId ) {

		Map<String, Object> groupByDateMap = new HashMap<>( ) ;

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			// if ( !serviceNameFilter.contains( MULTIPLE_SERVICE_DELIMETER ) )
			// {
			groupByDateMap.put( "serviceName", "$serviceName" ) ;

			// }
		}

		groupByDateMap.put( "appId", "$appId" ) ;
		groupByDateMap.put( "project", "$project" ) ;
		groupByDateMap.put( "lifecycle", "$lifecycle" ) ;

		if ( byHost ) {

			groupByDateMap.put( "host", "$host" ) ;

		}

		Document groupServiceData = new Document( "_id", new BasicDBObject( groupByDateMap ) ) ;
		groupServiceData.append( "date", new Document( "$push", "$date" ) ) ;

		for ( String key : metricsId ) {

			groupServiceData.append( key, new Document( "$push", "$" + key ) ) ;

		}

		if ( metricsId.length > 1 ) {

			groupServiceData.append( "total", new Document( "$push", "$total" ) ) ;

		}

		Document groupData = new Document( "$group", groupServiceData ) ;
		return groupData ;

	}

	private Document trendingProjectionByLifeBuilder ( String[] metricsId ) {

		Document projectLife = new Document( ) ;

		for ( String key : metricsId ) {

			projectLife.append( key, 1 ) ;

		}

		if ( metricsId.length > 1 ) {

			projectLife.append( "total", 1 ) ;

		}

		projectLife.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", "$_id.date" )
				.append( "_id", 0 ) ;
		Document projectLifeOp = new Document( "$project", projectLife ) ;
		return projectLifeOp ;

	}

	private Document trendingGroupByLifeBuilder ( String[] metricsId ) {

		Map<String, Object> groupByLifeMap = new HashMap<>( ) ;

		groupByLifeMap.put( "appId", "$appId" ) ;
		groupByLifeMap.put( "project", "$project" ) ;
		groupByLifeMap.put( "lifecycle", "$lifecycle" ) ;
		groupByLifeMap.put( "date", "$date" ) ;
		Document groupByLifeFields = new Document( "_id", new BasicDBObject( groupByLifeMap ) ) ;

		for ( String key : metricsId ) {

			groupByLifeFields.append( key, new Document( "$sum", "$" + key ) ) ;

		}

		if ( metricsId.length > 1 ) {

			groupByLifeFields.append( "total", new Document( "$sum", "$total" ) ) ;

		}

		Document groupByLife = new Document( "$group", groupByLifeFields ) ;
		return groupByLife ;

	}

	private void trendingDivideByBuilder (
											boolean byHost ,
											String[] metricsId ,
											String[] divideBy ,
											List<Document> trendingQueryOperations ) {

		for ( String divisorStr : divideBy ) {

			if ( metricsId.length == 1 ) {

				Document numSampleProjection = new Document( ) ;
				List<Object> numSampleDivideList = new ArrayList<>( ) ;
				numSampleDivideList.add( "$" + metricsId[0] ) ;

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
						.append( "serviceName", 1 )
						.append( "numberOfSamples", 1 )
						.append( "date", 1 ) ;

				if ( byHost ) {

					numSampleProjection.append( "host", 1 ) ;

				}

				numSampleProjection.append( metricsId[0], new Document( "$divide", numSampleDivideList ) ) ;
				Document projectDivision = new Document( "$project", numSampleProjection ) ;

				if ( numSampleDivideList.size( ) == 2 ) {

					trendingQueryOperations.add( projectDivision ) ;

				}

			} else if ( metricsId.length > 1 ) {

				Document numSampleProjection = new Document( ) ;
				List<Object> numSampleDivideList = new ArrayList<>( ) ;
				numSampleDivideList.add( "$total" ) ;

				if ( divisorStr.equalsIgnoreCase( "numberOfSamples" ) ) {

					numSampleDivideList.add( "$numberOfSamples" ) ;

				} else {

					if ( NumberUtils.isNumber( divisorStr ) ) {

						numSampleDivideList.add( Double.parseDouble( divisorStr ) ) ;

					}

				}

				for ( String key : metricsId ) {

					numSampleProjection.append( key, 1 ) ;

				}

				numSampleProjection.append( "appId", 1 )
						.append( "lifecycle", 1 )
						.append( "project", 1 )
						.append( "serviceName", 1 )
						.append( "numberOfSamples", 1 )
						.append( "date", 1 ) ;

				if ( byHost ) {

					numSampleProjection.append( "host", 1 ) ;

				}

				numSampleProjection.append( "total", new BasicDBObject( "$divide", numSampleDivideList ) ) ;
				Document projectDivison = new Document( "$project", numSampleProjection ) ;

				if ( numSampleDivideList.size( ) == 2 ) {

					trendingQueryOperations.add( projectDivison ) ;

				}

			}

		}

	}

	private Document trendingProjectionConditionBuilder ( boolean byHost , String[] metricsId ) {

		Document trendingProjectionCondition = new Document( ) ;
		trendingProjectionCondition.append( "appId", 1 )
				.append( "lifecycle", 1 )
				.append( "project", 1 )
				.append( "serviceName", 1 )
				// .append("numberOfSamples", 1)
				.append( "date", 1 ) ;

		if ( byHost ) {

			trendingProjectionCondition.append( "host", 1 ) ;

		}

		for ( String key : metricsId ) {

			trendingProjectionCondition.append( key, 1 ) ;

		}

		if ( metricsId.length > 1 ) {

			trendingProjectionCondition.append( "total", 1 ) ;

		}

		ArrayList trueCondition = new ArrayList( ) ;
		trueCondition.add( "$numberOfSamples" ) ;
		trueCondition.add( 0 ) ;
		ArrayList trueCondArray = new ArrayList( ) ;
		trueCondArray.add( new Document( "$eq", trueCondition ) ) ;
		trueCondArray.add( 1 ) ;
		trueCondArray.add( "$numberOfSamples" ) ;
		Document condition = new Document( "$cond", trueCondArray ) ;
		trendingProjectionCondition.append( "numberOfSamples", condition ) ;
		Document trendingProjectionConitionOperation = new Document( "$project", trendingProjectionCondition ) ;
		return trendingProjectionConitionOperation ;

	}

	private Document trendingProjectionBuilder ( boolean byHost , String[] metricsId , String[] divideBy ) {

		Document trendingProjection = new Document( ) ; // used to filter output
		List<Object> metricsList = new ArrayList<>( ) ;

		for ( String key : metricsId ) {

			metricsList.add( "$" + key ) ;
			trendingProjection.append( key, 1 ) ;

		}

		if ( metricsId.length > 1 ) {

			trendingProjection.append( "total", new Document( "$add", metricsList ) ) ;

		}

		trendingProjection.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "serviceName", "$_id.serviceName" )
				.append( "date", "$_id.date" )
				.append( "_id", 0 ) ;

		if ( byHost ) {

			trendingProjection.append( "host", "$_id.host" ) ;

		}

		if ( ArrayUtils.contains( divideBy, "numberOfSamples" ) ) {

			trendingProjection.append( "numberOfSamples", 1 ) ;

		}

		Document projectionOperation = new Document( "$project", trendingProjection ) ;
		return projectionOperation ;

	}

	private Document trendingPrimaryQueryBuilder (
													String appId ,
													String project ,
													String life ,
													String serviceNameFilter ,
													String category ,
													int numDays ,
													int dateOffSet ) {

		Document trendQuery = constructDocumentQuery( appId, project, life, category ) ;
		trendQuery = addDateToDocumentQuery( trendQuery, numDays, dateOffSet ) ;

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			List<Document> serviceList = new ArrayList<Document>( ) ;

			for ( String name : serviceNameFilter.split( MULTIPLE_SERVICE_DELIMETER ) ) {

				serviceList.add( new Document( "serviceName", name ) ) ;

			}

			Document anyServiceMatch = new Document( "$or", serviceList ) ;
			trendQuery.append( "data.summary", new Document( "$elemMatch", anyServiceMatch ) ) ;

		}

		Document op1_global_match = new Document( "$match", trendQuery ) ;
		return op1_global_match ;

	}

}
