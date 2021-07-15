package org.csap.events.http.ui.rest ;

import java.io.PrintWriter ;
import java.util.List ;
import java.util.Map ;
import java.util.Set ;
import java.util.concurrent.TimeUnit ;

import javax.inject.Inject ;
import javax.servlet.http.HttpServletRequest ;

import org.apache.commons.lang3.ArrayUtils ;
import org.apache.commons.lang3.StringUtils ;
import org.bson.Document ;
import org.csap.docs.CsapDoc ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.EventJsonConstants ;
import org.csap.events.db.AnalyticsDbReader ;
import org.csap.events.db.AnalyticsHelper ;
import org.csap.events.db.CsapAdoptionReportBuilder ;
import org.csap.events.db.GlobalAnalyticsDbReader ;
import org.csap.events.db.TrendingReportHelper ;
import org.csap.events.util.BusinessProgramDisplayInfo ;
import org.csap.events.util.GraphData ;
import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapApplication ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.cache.annotation.Cacheable ;
import org.springframework.http.MediaType ;
import org.springframework.web.bind.annotation.RequestMapping ;
import org.springframework.web.bind.annotation.RequestParam ;
import org.springframework.web.bind.annotation.RestController ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.client.AggregateIterable ;

@RestController
@RequestMapping ( CsapEventsApplication.REPORT_API )
@CsapDoc ( title = "CSAP reports API" , type = CsapDoc.PUBLIC , notes = {
		"CSAP analytics reports api",
		"<a class='csap-link' target='_blank' href='https://github.com/csap-platform/csap-core/wiki'>learn more</a>"
} )
public class ApiReportController {
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	private static final String REPORT_DATA_ALL_SUMMARY = "csap.reports-summary" ;
	private static final String REPORT_DATA_ALL_TREND = "csap.reports-trend" ;
	public String service = "service" ;

	@Inject
	private AnalyticsDbReader analyticsDbReader ;

	@Inject
	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	@Inject
	private AnalyticsHelper analyticsHelper ;

	@Inject
	private CsapAdoptionReportBuilder adoptionReportBuilder ;

	@Inject
	private GlobalAnalyticsDbReader globalAnalyticsReader ;

	@CsapDoc ( notes = "Get host and service report " , linkTests = {
			"JSON", "JSONP"
	} , linkGetParams = {
			"a=b",
			"callback=myTestFunction"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE, "application/javascript"
	} )
	@RequestMapping ( value = "/reportCounts" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode reportCounts ( ) {

		return globalAnalyticsReader.reportCounts( ) ;

	}

	@CsapDoc ( notes = "Get global analytics report" )
	@RequestMapping ( value = "/adoption/current" , produces = MediaType.APPLICATION_JSON_VALUE )
	public JsonNode adoptionCurrent (
										@RequestParam ( value = "numDays" , required = false , defaultValue = "7" ) Integer numDays )
		throws Exception {

		AggregateIterable<Document> results = globalAnalyticsReader.getAnalyticsData( numDays ) ;
		return EventJsonConstants.transformToJackson( results ) ;

	}

	@CsapDoc ( notes = "graph data for global analytics" )
	@RequestMapping ( value = "/adoption/trends" , produces = MediaType.APPLICATION_JSON_VALUE )
	public GraphData adoptionTrends (
										@RequestParam ( value = "numDays" , required = false , defaultValue = "30" ) Integer numDays ) {

		GraphData graphdata = globalAnalyticsReader.buildProjectAdoptionTrends( numDays ) ;
		return graphdata ;

	}

	@CsapDoc ( notes = "Health of system" )
	@RequestMapping ( value = "/health" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Map<String, Map> getHealthMessages ( )
		throws Exception {

		Map<String, Map> results = globalAnalyticsReader.getHealthInfo( ) ;
		return results ;

	}

	@CsapDoc ( notes = "Health of project" , linkTests = {
			"projectName='CSAP Engineering'"
	} , linkGetParams = {
			"projectName='CSAP Engineering'"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@RequestMapping ( value = "/healthMessage" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String getHealthMetrics ( @RequestParam ( value = "projectName" , required = true ) String projectName )
		throws Exception {

		return globalAnalyticsReader.getHealthErrorMessages( projectName ) ;

	}

	@CsapDoc ( notes = "Instance information" , linkTests = {
			"projectName='CSAP Engineering',appId=csapeng.gen"
	} , linkGetParams = {
			"projectName='CSAP Engineering',appId=csapeng.gen"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@RequestMapping ( value = "/package-summary" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Map<String, Document> getInstanceInfo (
													@RequestParam ( value = "projectName" , required = true ) String projectName ,
													@RequestParam ( value = "appId" , required = true ) String appId )
		throws Exception {

		return globalAnalyticsReader.getPackageSumnmarysByLife( projectName, appId ) ;

	}

	@CsapDoc ( notes = "Display names for project" )
	@RequestMapping ( value = "/displayNames" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Map<String, Document> getDisplayNames ( ) {

		return analyticsHelper.getAnalyticsSettings( ) ;

	}

	@CsapDoc ( notes = "Display information.Deprecated will be removed" )
	@RequestMapping ( value = "/displayInformation" , produces = MediaType.APPLICATION_JSON_VALUE )
	public Map<String, BusinessProgramDisplayInfo> getDisplayNameInformation ( ) {

		return analyticsHelper.getDisplayNameInfo( ) ;

	}

	@CsapDoc ( notes = "This is deprecated. Will be removed" )
	@RequestMapping ( value = "/saveShowHide" , produces = MediaType.APPLICATION_JSON_VALUE )
	public void setShowHide (
								@RequestParam ( value = "packageName" , defaultValue = "" ) String packageName ,
								@RequestParam ( value = "isHidden" , defaultValue = "false" ) boolean isHidden ) {

		analyticsHelper.saveOrUpdateHide( packageName, isHidden ) ;

	}

	@CsapDoc ( notes = "This is depreaceted will be removed" )
	@RequestMapping ( value = "/saveHealthMessage" , produces = MediaType.APPLICATION_JSON_VALUE )
	public void saveHealthMessagetValue (
											@RequestParam ( value = "packageName" , defaultValue = "" ) String packageName ,
											@RequestParam ( value = "life" , defaultValue = "" ) String life ,
											@RequestParam ( value = "saveHealthMessage" , defaultValue = "false" ) boolean saveHealthMessage ) {

		analyticsHelper.saveOrUpdateHealth( packageName, life, saveHealthMessage ) ;

	}

	@CsapDoc ( notes = "This is deprecated will be removed" )
	@RequestMapping ( value = "/saveDisplayName" , produces = MediaType.APPLICATION_JSON_VALUE )
	public void setDisplayName (
									@RequestParam ( value = "packageName" , defaultValue = "" ) String packageName ,
									@RequestParam ( value = "displayName" , defaultValue = "" ) String displayName ) {

		analyticsHelper.saveOrUpdateDisplayName( packageName, displayName ) ;

	}

	@CsapDoc ( notes = "user id report" , linkTests = {
			"project='CSAP Engineering'",
			"project='CSAP Engineering',trending=true"
	} , linkGetParams = {
			"project='CSAP Engineering'",
			"project='CSAP Engineering',trending=true"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	// @Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	// @Cacheable(value = "userIdReports")
	@RequestMapping ( value = "/userid" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode userIdReports (
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "project" , required = false ) String project ,
										@RequestParam ( value = "life" , required = false ) String life ,
										@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
										@RequestParam ( value = "numHours" , required = false , defaultValue = "0" ) Integer hours ,
										@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
										@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ) {

		ObjectNode userReport = jacksonMapper.createObjectNode( ) ;

		try {

			AggregateIterable<Document> results ;

			if ( trending ) {

				var timer = metricUtilities.startTimer( ) ;
				results = analyticsDbReader.userActivityTrendingReport( appId, project, life, days,
						dateOffSet ) ;

				var nanos = metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;
				logger.info( "User Trending: {}", CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ) ) ;

			} else {

				var timer = metricUtilities.startTimer( ) ;
				results = analyticsDbReader.userActivityReport( appId, project, life, days ) ;

				var nanos = metricUtilities.stopTimer( timer, REPORT_DATA_ALL_SUMMARY ) ;
				logger.info( "User Activity: {}", CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ) ) ;

			}

			userReport.set( "data", EventJsonConstants.transformToJackson( results ) ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "userreport", null ) ;
				userReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;

		}

		return userReport ;

	}

	@CsapDoc ( notes = "vm report" , linkTests = {
			"project='CSAP Engineering'",
			"project='CSAP Engineering',trending=true"
	} , linkGetParams = {
			"project='CSAP Engineering'",
			"project='CSAP Engineering',trending=true"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = {
			"vm", CsapApplication.COLLECTION_HOST
	} , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode buildHostReport (
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "project" , required = false ) String project ,
										@RequestParam ( value = "life" , required = false ) String life ,
										@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
										@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
										@RequestParam ( value = "resource" , required = false , defaultValue = "resource_30" ) String resource ,
										@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
										@RequestParam ( value = "metricsId" , required = false ) String[] metricsId ,
										@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
										@RequestParam ( value = "allVmTotal" , required = false ) String allVmTotal ,
										@RequestParam ( value = "divideBy" , required = false ) String[] divideBy ,
										@RequestParam ( value = "top" , required = false , defaultValue = "0" ) int top ,
										@RequestParam ( value = "low" , required = false , defaultValue = "0" ) int low ) {

		ObjectNode hostReport = jacksonMapper.createObjectNode( ) ;

		try {

			if ( trending && ArrayUtils.isNotEmpty( metricsId ) ) {

				var timer = metricUtilities.startTimer( ) ;
				var results = analyticsDbReader.buildHostTrendReport( appId, project, life, metricsId, divideBy,
						allVmTotal, perVm, top, low, days, dateOffSet ) ;

				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;
				hostReport.set( "data", results ) ;

			} else {

				var timer = metricUtilities.startTimer( ) ;
				AggregateIterable<Document> results = analyticsDbReader.getVmReport( appId, project, life, days,
						dateOffSet ) ;

				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_SUMMARY ) ;
				hostReport.set( "data", EventJsonConstants.transformToJackson( results ) ) ;

			}

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "hostreport", null ) ;
				hostReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while building report {} ", CSAP.buildCsapStack( e ) ) ;

		}

		return hostReport ;

	}

	@Inject
	private TrendingReportHelper trendingReportHelper ;

	@CsapDoc ( notes = "Top hosts" , linkTests = {
			"project='CSAP Engineering',metricsId=process.topCpu,hosts=2,appId=csapeng.gen"
	} , linkGetParams = {
			"project='CSAP Engineering',metricsId=process.topCpu,hosts=2,appId=csapeng.gen"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = "/top" , produces = MediaType.APPLICATION_JSON_VALUE )
	public List topHosts (
							@RequestParam ( value = "appId" , required = true ) String appId ,
							@RequestParam ( value = "project" , required = false ) String project ,
							@RequestParam ( value = "life" , required = false ) String life ,
							@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
							@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
							@RequestParam ( value = "hosts" , required = false , defaultValue = "0" ) int numHosts ,
							@RequestParam ( value = "metricsId" , required = true ) String[] metricsId ,
							@RequestParam ( value = "reportType" , required = false , defaultValue = "topReport" ) String reportType ) {

		List results = null ;

		try {

			var timer = metricUtilities.startTimer( ) ;

			String category = getCategory( metricsId[0] ) ;
			String[] actualMetricsId = getActualMetricsId( metricsId ) ;
			boolean unwindSummary = unwindSummary( metricsId[0] ) ;
			String serviceNameFilter = getServiceNameFilter( metricsId[0] ) ;
			String[] divideBy = getDivideBy( metricsId[0] ) ;
			results = trendingReportHelper.topHosts( appId, project, life,
					actualMetricsId, divideBy, numHosts,
					-1, category, serviceNameFilter, unwindSummary, days, dateOffSet ) ;
			metricUtilities.stopTimer( timer, "topReport" ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;

		}

		return results ;

	}

	@CsapDoc ( notes = "Low hosts" , linkTests = {
			"project='CSAP Engineering',metricsId=process.topCpu,hosts=2,appId=csapeng.gen"
	} , linkGetParams = {
			"project='CSAP Engineering',metricsId=process.topCpu,hosts=2,appId=csapeng.gen"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = "/low" , produces = MediaType.APPLICATION_JSON_VALUE )
	public List lowHosts (
							@RequestParam ( value = "appId" , required = true ) String appId ,
							@RequestParam ( value = "project" , required = false ) String project ,
							@RequestParam ( value = "life" , required = false ) String life ,
							@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
							@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
							@RequestParam ( value = "hosts" , required = false , defaultValue = "0" ) int numHosts ,
							@RequestParam ( value = "metricsId" , required = true ) String[] metricsId ,
							@RequestParam ( value = "reportType" , required = false , defaultValue = "lowReport" ) String reportType ) {

		List results = null ;

		try {

			var timer = metricUtilities.startTimer( ) ;

			String category = getCategory( metricsId[0] ) ;
			String[] actualMetricsId = getActualMetricsId( metricsId ) ;
			boolean unwindSummary = unwindSummary( metricsId[0] ) ;
			String serviceNameFilter = getServiceNameFilter( metricsId[0] ) ;
			String[] divideBy = getDivideBy( metricsId[0] ) ;
			results = trendingReportHelper.topHosts( appId, project, life,
					actualMetricsId, divideBy, numHosts,
					1, category, serviceNameFilter, unwindSummary, days, dateOffSet ) ;

			metricUtilities.stopTimer( timer, "lowReport" ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;

		}

		return results ;

	}

	private String[] getDivideBy ( String metricsId ) {

		if ( metricsId.startsWith( "health." ) ) {

			return null ;

		} else {

			return new String[] {
					"numberOfSamples"
			} ;

		}

	}

	private boolean unwindSummary ( String metricsId ) {

		if ( metricsId.startsWith( "vm." ) || metricsId.startsWith( "health." ) ) {

			return false ;

		} else {

			return true ;

		}

	}

	private String getServiceNameFilter ( String metricsId ) {

		String serviceNameFilter = "" ;

		if ( metricsId.startsWith( "process." ) || metricsId.startsWith( "jmx." ) ) {

			if ( metricsId.contains( "_" ) ) {

				serviceNameFilter = metricsId.substring( metricsId.indexOf( "_" ) + 1 ) ;

			}

		} else if ( metricsId.startsWith( "jmxCustom." ) ) {

			String[] metricsIdArr = metricsId.split( "\\." ) ;

			if ( metricsIdArr.length >= 3 ) {

				serviceNameFilter = metricsIdArr[1] ;

			}

		}

		return serviceNameFilter ;

	}

	private String[] getActualMetricsId ( String[] metricsId ) {

		String[] actualMetricsId = new String[metricsId.length] ;

		if ( metricsId[0].startsWith( "vm." ) || metricsId[0].startsWith( "health." ) ) {

			if ( metricsId[0].contains( "coresActive" ) ) {

				actualMetricsId = new String[2] ;
				actualMetricsId[0] = "totalUsrCpu" ;
				actualMetricsId[1] = "totalSysCpu" ;

			} else {

				for ( int i = 0; i < metricsId.length; i++ ) {

					String key = metricsId[i] ;
					String[] keyArr = key.split( "\\." ) ;

					if ( keyArr.length == 2 ) {

						actualMetricsId[i] = keyArr[1] ;

					}

				}

			}

		} else if ( metricsId[0].startsWith( "process." ) || metricsId[0].startsWith( "jmx." ) ) {

			// 1. process 2. attribute name 3. _serviceName
			for ( int i = 0; i < metricsId.length; i++ ) {

				String metricsKey = metricsId[i] ;
				String actualMetricsKey = "" ;

				if ( metricsKey.contains( "_" ) ) {

					actualMetricsKey = metricsKey.substring( ( metricsKey.indexOf( "." ) + 1 ), metricsKey.indexOf(
							"_" ) ) ;

				} else {

					actualMetricsKey = metricsKey.substring( metricsKey.indexOf( "." ) + 1 ) ;

				}

				if ( StringUtils.isNotBlank( actualMetricsKey ) ) {

					actualMetricsId[i] = actualMetricsKey ;

				}

			}

		} else if ( metricsId[0].startsWith( "jmxCustom." ) ) {

			// 1. jmxCustom 2.service name 3.metrics ID
			for ( int i = 0; i < metricsId.length; i++ ) {

				String metricsKey = metricsId[i] ;
				String[] metricsKeyArr = metricsKey.split( "\\." ) ;

				if ( metricsKeyArr.length >= 3 ) {

					actualMetricsId[i] = metricsKeyArr[2] ;

				}

			}

		}

		return actualMetricsId ;

	}

	private String getCategory ( String metricsId ) {

		if ( metricsId.startsWith( "vm." ) ) {

			return "/csap/reports/host/daily" ;

		} else if ( metricsId.startsWith( "process." ) ) {

			return "/csap/reports/process/daily" ;

		} else if ( metricsId.startsWith( "jmxCustom." ) ) {

			return "/csap/reports/jmxCustom/daily" ;

		} else if ( metricsId.startsWith( "jmx." ) ) {

			return "/csap/reports/jmx/daily" ;

		} else if ( metricsId.startsWith( "health." ) ) {

			return "/csap/reports/health" ;

		}

		return "" ;

	}

	@CsapDoc ( notes = "Get core report " , linkTests = {
			"appId=csapeng.gen", "appId=csapeng.gen,top=1"
	} , linkGetParams = {
			"appId=csapeng.gen",
			"appId=csapeng.gen,top=1"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_JSON_VALUE
	} )
	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = "/custom/core" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode coreTrending (
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "project" , required = false ) String project ,
										@RequestParam ( value = "life" , required = false ) String life ,
										@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
										@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
										@RequestParam ( value = "resource" , required = false , defaultValue = "resource_30" ) String resource ,
										@RequestParam ( value = "trending" , required = true , defaultValue = "true" ) Boolean trending ,
										@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
										@RequestParam ( value = "top" , required = false , defaultValue = "0" ) int top ,
										@RequestParam ( value = "low" , required = false , defaultValue = "0" ) int low ) {

		var timer = metricUtilities.startTimer( ) ;
		ObjectNode coreReport = jacksonMapper.createObjectNode( ) ;

		try {

			String[] metricsId = {
					"totalSysCpu", "totalUsrCpu"
			} ;
			var coreTrendReport = analyticsDbReader.buildCoreUsedTrendingReport( appId, project, life, metricsId,
					perVm, top,
					low, days, dateOffSet ) ;

			coreReport.set( "data", coreTrendReport ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "hostreport", null ) ;
				coreReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;

		}

		metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;
		return coreReport ;

	}

	@CsapDoc ( notes = "Get health report " , linkTests = {
			"appId=csapeng.gen", "appId=csapeng.gen,top=1"
	} , linkGetParams = {
			"appId=csapeng.gen",
			"appId=csapeng.gen,top=1"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_JSON_VALUE
	} )
	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = "/custom/health" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode healthTrending (
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "project" , required = false ) String project ,
										@RequestParam ( value = "life" , required = false ) String life ,
										@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
										@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
										@RequestParam ( value = "trending" , required = true , defaultValue = "true" ) Boolean trending ,
										@RequestParam ( value = "perVm" , required = true , defaultValue = "false" ) Boolean perVm ,
										@RequestParam ( value = "top" , required = false , defaultValue = "0" ) int top ,
										@RequestParam ( value = "low" , required = false , defaultValue = "0" ) int low ,
										@RequestParam ( value = "category" , required = false , defaultValue = "/csap/reports/health" ) String category ) {

		var timer = metricUtilities.startTimer( ) ;
		ObjectNode healthReport = jacksonMapper.createObjectNode( ) ;

		try {

			AggregateIterable<Document> results = analyticsDbReader.getVmHealthReport( appId, project, life,
					perVm, top, low,
					days, dateOffSet, category ) ;

			healthReport.set( "data", EventJsonConstants.transformToJackson( results ) ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "hostreport", null ) ;
				healthReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;

		}

		metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;
		return healthReport ;

	}

	@CsapDoc ( notes = "Get log rotate report " , linkTests = {
			"appId=csapeng.gen,metricsId=MeanSeconds",
			"appId=csapeng.gen,serviceName=data,metricsId=MeanSeconds"
	} , linkGetParams = {
			"appId=csapeng.gen,metricsId=MeanSeconds",
			"appId=csapeng.gen,serviceName=data,metricsId=MeanSeconds"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE,
			MediaType.APPLICATION_JSON_VALUE
	} )
	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = "/custom/logRotate" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode logRotateTrending (
											@RequestParam ( value = "appId" , required = false ) String appId ,
											@RequestParam ( value = "project" , required = false ) String project ,
											@RequestParam ( value = "life" , required = false ) String life ,
											@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
											@RequestParam ( value = "serviceName" , required = false ) String serviceNameFilter ,
											@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
											@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
											@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
											@RequestParam ( value = "metricsId" , required = false ) String[] metricsId ,
											@RequestParam ( value = "divideBy" , required = false ) String[] divideBy ,
											@RequestParam ( value = "category" , required = false , defaultValue = "/csap/reports/logRotate" ) String category ) {

		var timer = metricUtilities.startTimer( ) ;
		ObjectNode logRotateReport = jacksonMapper.createObjectNode( ) ;

		try {

			var trendReport = analyticsDbReader.buildCategoryTrendingReport( perVm, appId, project, life,
					serviceNameFilter,
					category, metricsId, divideBy, null, days, dateOffSet ) ;

			if ( null != trendReport ) {

				logRotateReport.set( "data", trendReport ) ;

			}

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "logRotateReport",
						serviceNameFilter ) ;
				logRotateReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;

		}

		metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;
		return logRotateReport ;

	}

	@CsapDoc ( notes = "Service report" , linkTests = {
			"a=b",
			"appId=csapeng.gen,serviceName=data,metricsId=socketCount"
	} , linkGetParams = {
			"a=b",
			"appId=csapeng.gen,serviceName=data,metricsId=socketCount"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE,
			MediaType.APPLICATION_JSON_VALUE
	} )

	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = {
			"service", CsapApplication.COLLECTION_OS_PROCESS
	} , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode serviceReports (
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "project" , required = false ) String project ,
										@RequestParam ( value = "life" , required = false ) String life ,
										@RequestParam ( value = "host" , required = false ) String host ,
										@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
										@RequestParam ( value = "serviceName" , required = false ) String serviceNameFilter ,
										@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
										@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
										@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
										@RequestParam ( value = "metricsId" , required = false ) String[] metricsId ,
										@RequestParam ( value = "divideBy" , required = false ) String[] divideBy ,
										@RequestParam ( value = "allVmTotal" , required = false ) String allVmTotal ,
										@RequestParam ( value = "category" , required = false , defaultValue = "" ) String category ,
										HttpServletRequest httpRequest ) {

		if ( StringUtils.isEmpty( category ) ) {

			category = "/csap/reports/" + CsapApplication.COLLECTION_OS_PROCESS + "/daily" ;

			if ( httpRequest.getRequestURI( ).endsWith( "service" ) ) {

				category = "/csap/reports/process/daily" ;
				logger.warn( "{} legacy report: category: {}", appId, category ) ;

			}

		}

		logger.debug(
				"OS Process Report for host: {}, serviceNameFilter: {} \n\t number of Days: {} , offSet: {}, appId: {} , project: {}, \n\t category: {}",
				host, serviceNameFilter, days, dateOffSet, appId, project, category ) ;

		ObjectNode osProcessReport = jacksonMapper.createObjectNode( ) ;

		try {

			JsonNode requestedReport ;

			var isTrendByService = perVm
					&& trending
					&& ( metricsId != null )
					&& category.contains( CsapApplication.COLLECTION_OS_PROCESS )
					&& StringUtils.isEmpty( serviceNameFilter ) ;

			if ( isTrendByService ) {

				AggregateIterable<Document> results = analyticsDbReader.findSummaryReportDataUsingCategory(
						appId, project, life, null, serviceNameFilter, category,
						days, dateOffSet ) ;
				var summaryServicesReport = EventJsonConstants.transformToJackson( results ) ;

				var multiServiceReport = jacksonMapper.createArrayNode( ) ;
				var serviceCategory = category ;
				CSAP.jsonStream( summaryServicesReport )
						.filter( JsonNode::isObject )
						.map( summaryReport -> (ObjectNode) summaryReport )
						.forEach( summaryReport -> {

							var serviceName = summaryReport.path( "serviceName" ).asText( ) ;

							logger.debug( "Running report for: {}, metric: {}, ", serviceName, metricsId ) ;
//						if ( serviceName.equals( "csap-agent" ) || serviceName.equals( "csap-admin" ) ) {

							try {

								var serviceTrendPerVm = false ;
								var serviceReport = analyticsDbReader.buildCategoryTrendingReport(
										serviceTrendPerVm, appId, project, life, serviceName, serviceCategory,
										metricsId,
										divideBy,
										Boolean.toString( ! serviceTrendPerVm ), days, dateOffSet ) ;

								multiServiceReport.add( serviceReport.path( 0 ) ) ;

							} catch ( Exception e ) {

								logger.error( "Failed to build report for {}: {}", serviceCategory, CSAP.buildCsapStack(
										e ) ) ;

							}
//						}

						} ) ;

				requestedReport = multiServiceReport ;

			} else if ( trending ) {

				var timer = metricUtilities.startTimer( ) ;
				requestedReport = analyticsDbReader.buildCategoryTrendingReport( perVm, appId, project, life,
						serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;

			} else {

				var timer = metricUtilities.startTimer( ) ;
				AggregateIterable<Document> results = analyticsDbReader.findSummaryReportDataUsingCategory(
						appId, project, life, host, serviceNameFilter, category,
						days, dateOffSet ) ;
				requestedReport = EventJsonConstants.transformToJackson( results ) ;

				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_SUMMARY ) ;

			}

			osProcessReport.set( "data", requestedReport ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, category,
						serviceNameFilter ) ;
				osProcessReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Failed to build report for {}: {}", category, CSAP.buildCsapStack( e ) ) ;

		}

		return osProcessReport ;

	}

	@CsapDoc ( notes = "Service detail report" , linkTests = {
			"a=b", "appId=csapeng.gen,serviceName=data,metricsId=socketCount",
			"appId=csapeng.gen,serviceName=data,metricsId=socketCount,trending=true"
	} , linkGetParams = {
			"a=b",
			"appId=csapeng.gen,serviceName=data,metricsId=socketCount",
			"appId=csapeng.gen,serviceName=data,metricsId=socketCount,trending=true"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_JSON_VALUE
	} )
	@Cacheable ( CsapEventsApplication.DETAILS_REPORT_CACHE )
	@RequestMapping ( value = {
			"service/detail",
			CsapApplication.COLLECTION_OS_PROCESS + "/detail"
	} , //
			produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode osProcessDetailReport (
												@RequestParam ( value = "appId" , required = false ) String appId ,
												@RequestParam ( value = "project" , required = false ) String project ,
												@RequestParam ( value = "life" , required = false ) String life ,
												@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
												@RequestParam ( value = "serviceName" , required = false ) String serviceNameFilter ,
												@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
												@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
												@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
												@RequestParam ( value = "metricsId" , required = false ) String[] metricsId ,
												@RequestParam ( value = "divideBy" , required = false ) String[] divideBy ,
												@RequestParam ( value = "allVmTotal" , required = false ) String allVmTotal ,
												@RequestParam ( value = "category" , required = false , defaultValue = "" ) String category ,
												HttpServletRequest httpRequest ) {

		logger.debug( "appId: {}, project:{}, life: {}", appId, project, life ) ;

		if ( StringUtils.isEmpty( category ) ) {

			category = "/csap/reports/" + CsapApplication.COLLECTION_OS_PROCESS + "/daily" ;

			if ( httpRequest.getRequestURI( ).endsWith( "service/detail" ) ) {

				category = "/csap/reports/process/daily" ;
				logger.warn( "{} legacy report: category: {}", appId, category ) ;

			}

		}

		ObjectNode serviceDetailReport = jacksonMapper.createObjectNode( ) ;

		try {

			JsonNode requestedReport = null ;

			if ( trending ) {

				var timer = metricUtilities.startTimer( ) ;
				requestedReport = analyticsDbReader.buildCategoryTrendingReport( perVm, appId, project, life,
						serviceNameFilter, category,
						metricsId,
						divideBy, allVmTotal, days, dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;

			} else {

				var timer = metricUtilities.startTimer( ) ;
				AggregateIterable<Document> results = analyticsDbReader.findDetailReportDataUsingCategory( appId,
						project, life,
						serviceNameFilter, category, days,
						dateOffSet ) ;
				requestedReport = EventJsonConstants.transformToJackson( results ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_SUMMARY ) ;

			}

			serviceDetailReport.set( "data", requestedReport ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "servicereport",
						serviceNameFilter ) ;
				serviceDetailReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Failed to build report for {}: {}", category, CSAP.buildCsapStack( e ) ) ;

		}

		return serviceDetailReport ;

	}

	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = {
			"jmx", CsapApplication.COLLECTION_JAVA
	} , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode javaReports (
									@RequestParam ( value = "appId" , required = false ) String appId ,
									@RequestParam ( value = "project" , required = false ) String project ,
									@RequestParam ( value = "life" , required = false ) String life ,
									@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
									@RequestParam ( value = "host" , required = false ) String host ,
									@RequestParam ( value = "serviceName" , required = false ) String serviceNameFilter ,
									@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
									@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
									@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
									@RequestParam ( value = "metricsId" , required = false ) String[] metricsId ,
									@RequestParam ( value = "divideBy" , required = false ) String[] divideBy ,
									@RequestParam ( value = "allVmTotal" , required = false ) String allVmTotal ,
									@RequestParam ( value = "category" , required = false , defaultValue = "" ) String category ,
									HttpServletRequest httpRequest ) {

		if ( StringUtils.isEmpty( category ) ) {

			category = "/csap/reports/" + CsapApplication.COLLECTION_JAVA + "/daily" ;

			if ( httpRequest.getRequestURI( ).endsWith( "jmx" ) ) {

				category = "/csap/reports/jmx/daily" ;
				logger.warn( "{} host: {}, service: {} legacy report: category: {}", appId, host, serviceNameFilter,
						category ) ;

			}

		}

		ObjectNode javaReport = jacksonMapper.createObjectNode( ) ;

		try {

			JsonNode requestedReport ;

			if ( trending ) {

				var timer = metricUtilities.startTimer( ) ;
				requestedReport = analyticsDbReader.buildCategoryTrendingReport( perVm, appId, project, life,
						serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;

			} else {

				var timer = metricUtilities.startTimer( ) ;
				AggregateIterable<Document> results = analyticsDbReader.findSummaryReportDataUsingCategory( appId,
						project, life, host,
						serviceNameFilter, category, days, dateOffSet ) ;

				requestedReport = EventJsonConstants.transformToJackson( results ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_SUMMARY ) ;

			}

			javaReport.set( "data", requestedReport ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, category,
						serviceNameFilter ) ;
				javaReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;
			javaReport.put( "error", "Error getting jmx data" ) ;

		}

		return javaReport ;

	}

	@Cacheable ( CsapEventsApplication.DETAILS_REPORT_CACHE )
	@RequestMapping ( value = {
			"jmx/detail",
			CsapApplication.COLLECTION_JAVA + "/detail"
	} , //
			produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode jmxDetailReports (
											@RequestParam ( value = "appId" , required = false ) String appId ,
											@RequestParam ( value = "project" , required = false ) String project ,
											@RequestParam ( value = "life" , required = false ) String life ,
											@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
											@RequestParam ( value = "serviceName" , required = false ) String serviceNameFilter ,
											@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
											@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
											@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
											@RequestParam ( value = "metricsId" , required = false ) String[] metricsId ,
											@RequestParam ( value = "divideBy" , required = false ) String[] divideBy ,
											@RequestParam ( value = "allVmTotal" , required = false ) String allVmTotal ,
											@RequestParam ( value = "category" , required = false , defaultValue = "" ) String category ,
											HttpServletRequest httpRequest ) {

		if ( StringUtils.isEmpty( category ) ) {

			category = "/csap/reports/" + CsapApplication.COLLECTION_JAVA + "/daily" ;

			if ( httpRequest.getRequestURI( ).endsWith( "jmx/detail" ) ) {

				category = "/csap/reports/jmx/daily" ;
				logger.warn( "{} legacy report: category: {}", appId, category ) ;

			}

		}

		// @RequestParam ( value = "category" , required = false , defaultValue =
		// "/csap/reports/jmx/daily" ) String category ) {
		ObjectNode jmxDetailReport = jacksonMapper.createObjectNode( ) ;

		try {

			JsonNode requestedReport ;

			if ( trending ) {

				var timer = metricUtilities.startTimer( ) ;

				requestedReport = analyticsDbReader.buildCategoryTrendingReport( perVm, appId, project, life,
						serviceNameFilter, category,
						metricsId,
						divideBy, allVmTotal, days, dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;

			} else {

				var timer = metricUtilities.startTimer( ) ;

				AggregateIterable<Document> results = analyticsDbReader.findDetailReportDataUsingCategory( appId,
						project, life,
						serviceNameFilter, category, days,
						dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_SUMMARY ) ;
				requestedReport = EventJsonConstants.transformToJackson( results ) ;

			}

			jmxDetailReport.set( "data", requestedReport ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, category,
						serviceNameFilter ) ;
				jmxDetailReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;

		}

		return jmxDetailReport ;

	}

	@Cacheable ( CsapEventsApplication.SIMPLE_REPORT_CACHE )
	@RequestMapping ( value = {
			"jmxCustom", CsapApplication.COLLECTION_APPLICATION
	} , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode applicationReportsJmxAndHttp (
														@RequestParam ( value = "appId" , required = false ) String appId ,
														@RequestParam ( value = "project" , required = false ) String project ,
														@RequestParam ( value = "life" , required = false ) String life ,
														@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
														@RequestParam ( value = "host" , required = false ) String host ,
														@RequestParam ( value = "serviceName" ) String serviceNameFilter ,
														@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
														@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
														@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
														@RequestParam ( value = "metricsId" , required = false ) String[] metricsId ,
														@RequestParam ( value = "divideBy" , required = false ) String[] divideBy ,
														@RequestParam ( value = "allVmTotal" , required = false ) String allVmTotal ,
														@RequestParam ( value = "category" , required = false , defaultValue = "" ) String category ,
														HttpServletRequest httpRequest ) {

		if ( StringUtils.isEmpty( category ) ) {

			category = "/csap/reports/" + CsapApplication.COLLECTION_APPLICATION + "/daily" ;

			if ( httpRequest.getRequestURI( ).endsWith( "jmxCustom" ) ) {

				category = "/csap/reports/jmxCustom/daily" ;
				logger.warn( "{} legacy report: category: {}", appId, category ) ;

			}

		}

		// @RequestParam ( value = "category" , required = false , defaultValue =
		// "/csap/reports/jmxCustom/daily" ) String category ) {
		ObjectNode applicationReport = jacksonMapper.createObjectNode( ) ;

		try {

			JsonNode requestedReport ;

			if ( trending ) {

				var timer = metricUtilities.startTimer( ) ;
				requestedReport = analyticsDbReader.buildCategoryTrendingReport( perVm, appId, project, life,
						serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;

			} else {

				var timer = metricUtilities.startTimer( ) ;
				AggregateIterable<Document> results = analyticsDbReader.findSummaryReportDataUsingCategory( appId,
						project, life, host,
						serviceNameFilter, category, days, dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_SUMMARY ) ;
				requestedReport = EventJsonConstants.transformToJackson( results ) ;

			}

			applicationReport.set( "data", requestedReport ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, category,
						serviceNameFilter ) ;
				applicationReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;
			applicationReport.put( "error", "Error getting jmx  custom data" ) ;

		}

		return applicationReport ;

	}

	@Cacheable ( CsapEventsApplication.DETAILS_REPORT_CACHE )
	@RequestMapping ( value = {
			"jmxCustom/detail",
			CsapApplication.COLLECTION_APPLICATION + "/detail"
	} , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode applicationDetailReportBuilder (
														@RequestParam ( value = "appId" , required = false ) String appId ,
														@RequestParam ( value = "project" , required = false ) String project ,
														@RequestParam ( value = "life" , required = false ) String life ,
														@RequestParam ( value = "perVm" , required = false , defaultValue = "false" ) Boolean perVm ,
														@RequestParam ( value = "serviceName" ) String serviceNameFilter ,
														@RequestParam ( value = "numDays" , required = false , defaultValue = "1" ) Integer days ,
														@RequestParam ( value = "dateOffSet" , required = false , defaultValue = "0" ) Integer dateOffSet ,
														@RequestParam ( value = "trending" , required = false , defaultValue = "false" ) Boolean trending ,
														@RequestParam ( value = "metricsId" , required = false ) String[] metricsId ,
														@RequestParam ( value = "divideBy" , required = false ) String[] divideBy ,
														@RequestParam ( value = "allVmTotal" , required = false ) String allVmTotal ,
														@RequestParam ( value = "category" , required = false , defaultValue = "" ) String category ,
														HttpServletRequest httpRequest ) {

		if ( StringUtils.isEmpty( category ) ) {

			category = "/csap/reports/" + CsapApplication.COLLECTION_APPLICATION + "/daily" ;

			if ( httpRequest.getRequestURI( ).endsWith( "jmxCustom/detail" ) ) {

				category = "/csap/reports/jmxCustom/daily" ;
				logger.warn( "{} legacy report: category: {}", appId, category ) ;

			}

		}

		ObjectNode applicationDetailReport = jacksonMapper.createObjectNode( ) ;

		try {

			JsonNode requestedReport ;

			if ( trending ) {

				var timer = metricUtilities.startTimer( ) ;
				requestedReport = analyticsDbReader.buildCategoryTrendingReport( perVm, appId, project, life,
						serviceNameFilter, category,
						metricsId,
						divideBy, allVmTotal, days, dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_TREND ) ;

			} else {

				var timer = metricUtilities.startTimer( ) ;
				AggregateIterable<Document> results = analyticsDbReader.findDetailReportDataUsingCategory( appId,
						project, life,
						serviceNameFilter, category, days,
						dateOffSet ) ;
				metricUtilities.stopTimer( timer, REPORT_DATA_ALL_SUMMARY ) ;
				requestedReport = EventJsonConstants.transformToJackson( results ) ;

			}

			applicationDetailReport.set( "data", requestedReport ) ;

			if ( null != appId && null != project && null != life ) {

				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, category,
						serviceNameFilter ) ;
				applicationDetailReport.put( "numDaysAvailable", count ) ;

			}

		} catch ( Exception e ) {

			logger.error( "Exception while loading data {}", CSAP.buildCsapStack( e ) ) ;

		}

		return applicationDetailReport ;

	}

	@CsapDoc ( notes = "Get attributes for service name " , linkTests = {
			"serviceName=data"
	} , linkGetParams = {
			"serviceName=data"
	} , produces = {
			MediaType.APPLICATION_JSON_VALUE
	} )
	@RequestMapping ( value = "/attributes" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String reportAttributes (
										@RequestParam ( value = "appId" , required = false ) String appId ,
										@RequestParam ( value = "project" , required = false ) String project ,
										@RequestParam ( value = "life" , required = false ) String life ,
										@RequestParam ( value = "serviceName" ) String serviceNameFilter ,
										@RequestParam ( value = "category" , required = false , defaultValue = "" ) String category ,
										HttpServletRequest httpRequest ) {

		if ( StringUtils.isEmpty( category ) ) {

			category = "/csap/reports/" + CsapApplication.COLLECTION_APPLICATION + "/daily" ;

			// category = "/csap/reports/jmxCustom/daily" ;
			if ( httpRequest.getRequestURI( ).endsWith( "jmxCustom/detail" ) ) {

				category = "/csap/reports/jmxCustom/daily" ;
				logger.warn( "{} legacy report: category: {}", appId, category ) ;

			}

		}

		// @RequestParam ( value = "category" , required = false , defaultValue =
		// "/csap/reports/jmxCustom/daily" ) String category ) {
		String result = "" ;

		try {

			Set<String> results = analyticsHelper.findReportDocumentAttributes( appId, project, life, category,
					serviceNameFilter ) ;
			result = jacksonMapper.writeValueAsString( results ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while converting data", e ) ;

		}

		return result ;

	}

	@RequestMapping ( value = "/postAnalytics" , produces = MediaType.TEXT_PLAIN_VALUE )
	public void postAnalytics (
								@RequestParam ( value = "offSet" , required = false , defaultValue = "1" ) Integer offSet ,
								PrintWriter outputWriter ) {

		outputWriter.println( "Starting Report for all projects - note this can take some time to complete" ) ;
		outputWriter.println( adoptionReportBuilder.buildAdoptionReportAndSaveToDB( offSet ) ) ;
		outputWriter.println( "Complete" ) ;

		// return "Posted analytics";
	}

	@RequestMapping ( value = "/postProjectAnalytics" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String postAnalyticsForAProject (
												@RequestParam ( value = "numDays" , defaultValue = "1" ) Integer numDays ,
												@RequestParam ( value = "projectName" ) String projectName ) {

		logger.info( "Project Name {} offset {}", projectName, numDays ) ;
		String result = "" ;

		for ( int offSet = 1; offSet <= numDays; offSet++ ) {

			result = adoptionReportBuilder.buildAdoptionReportAndSaveToDB( offSet, projectName ) ;

		}

		return result ;

	}

}
