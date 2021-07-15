package org.csap.events.health ;

import java.net.URI ;
import java.text.SimpleDateFormat ;
import java.util.ArrayList ;
import java.util.Date ;
import java.util.List ;
import java.util.Random ;
import java.util.concurrent.TimeUnit ;

import javax.annotation.PostConstruct ;

import org.apache.commons.lang3.StringUtils ;
import org.csap.alerts.AlertProcessor ;
import org.csap.events.CsapEventsApplication ;
import org.csap.helpers.CSAP ;
import org.csap.integations.CsapInformation ;
import org.csap.integations.CsapMicroMeter ;
import org.csap.integations.CsapPerformance ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.boot.context.properties.ConfigurationProperties ;
import org.springframework.context.annotation.Bean ;
import org.springframework.scheduling.annotation.Scheduled ;
import org.springframework.stereotype.Service ;
import org.springframework.web.client.RestTemplate ;
import org.springframework.web.util.UriComponentsBuilder ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ObjectNode ;

@Service
@ConfigurationProperties ( CsapEventsApplication.CONFIGURATION_PREFIX + ".health-monitoring" )
public class AnalyticsHealthMonitor {
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	private List<String> appIds = new ArrayList<>( ) ;
	private List<String> lifes = new ArrayList<>( ) ;

	private String eventDataServiceContext = "set_in_yaml" ;
	private String eventAnalyticServiceContext = "set_in_yaml" ;

	private int userEventIdleDays = 3 ;

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@PostConstruct
	public void showConfiguration ( ) {

		logger.info( "checking health every minute using appids: {} and lifes: {}", getAppIds( ), getLifes( ) ) ;
		;

	}

	@Bean
	public CsapPerformance.CustomHealth analyticsHealth ( ) {

		// Push any work into background thread to avoid blocking collection

		CsapPerformance.CustomHealth health = new CsapPerformance.CustomHealth( ) {

			@Autowired
			AlertProcessor alertProcessor ;

			@Override
			public boolean isHealthy ( ObjectNode healthReport )
				throws Exception {

				logger.debug( "Invoking custom health" ) ;

				getFailureReasons( ).forEach( reason -> {

					alertProcessor.addFailure( this, healthReport, reason ) ;

				} ) ;

				if ( getFailureReasons( ).size( ) > 0 )
					return false ;
				return true ;

			}

			@Override
			public String getComponentName ( ) {

				return AnalyticsHealthMonitor.class.getName( ) ;

			}
		} ;

		return health ;

	}

	private RestTemplate restTemplate ;
	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	@Autowired
	CsapInformation csapInformation ;

	public AnalyticsHealthMonitor ( RestTemplate restTemplate ) {

		super( ) ;
		this.restTemplate = restTemplate ;

	}

	private boolean isHealthy = true ;

	public Boolean getAnalyticsStatus ( ) {

		return isHealthy ;

	}

	@Scheduled ( fixedRateString = "${csap-events.health-monitoring.intervals-ms.metrics:30000}" , initialDelayString = "${csap-events.health-monitoring.intervals-ms.metrics:30000}" )
	public void monitorMetricsScheduler ( ) {

		logger.debug( "running metrics monitoring" ) ;

		if ( ! csapInformation.getHttpPort( ).equals( "-1" ) ) {

			monitorMetrics( ) ;

		}

	}

	public void monitorMetrics ( ) {

		// Configurator.setLevel( AnalyticsHealthMonitor.class.getName(),
		// Level.DEBUG );

		List<String> latestIssues = new ArrayList<>( ) ;

		logger.debug( "calling metrics and latest event api" ) ;
		String[] appIdLifeResourceCategory = getAppIdLifeReportPattern( ) ;

		var allTimer = metricUtilities.startTimer( ) ;

		var metricsTimer = metricUtilities.startTimer( ) ;
		latestIssues.addAll( queryLatestMetrics( appIdLifeResourceCategory ) ) ;
		metricUtilities.stopTimer( metricsTimer, "monitor.performance" ) ;

		var userTimer = metricUtilities.startTimer( ) ;
		latestIssues.addAll( queryLatestUserEvents( appIdLifeResourceCategory ) ) ;
		metricUtilities.stopTimer( userTimer, "monitor.user" ) ;

		var analyticsTimer = metricUtilities.startTimer( ) ;
		latestIssues.addAll( queryUserAnalytics( appIdLifeResourceCategory ) ) ;
		metricUtilities.stopTimer( analyticsTimer, "monitor.analytics" ) ;

		var nanos = metricUtilities.stopTimer( allTimer, "monitor.all" ) ;

		failureReasons = latestIssues ;

	}

	List<String> queryUserAnalytics ( String[] appIdLifeResourceCategory ) {

		return new ArrayList<>( ) ;

	}

	SimpleDateFormat formatter = new SimpleDateFormat( "MM/dd/yyyy" ) ;

	List<String> queryLatestUserEvents ( String[] appIdLifeResourceCategory ) {

		List<String> userEventIssues = new ArrayList<>( ) ;

		if ( userEventIdleDays < 1 ) {

			logger.debug( "Query latest user event disabled." ) ;
			return userEventIssues ;

		}

		Date idleDay = new Date( System.currentTimeMillis( ) - TimeUnit.DAYS.toMillis( userEventIdleDays ) ) ;
		String from = ",from=" + formatter.format( idleDay ) ;
		String to = ",to=" + formatter.format( System.currentTimeMillis( ) ) ;

		// event ui has custom params used for querying from ui
		// csapInformation.getLoadBalancerUrl()
		URI eventUiUri = UriComponentsBuilder
				.fromHttpUrl( "http://localhost:" + csapInformation.getHttpPort( ) + csapInformation.getHttpContext( ) )
				.path( "/api/event" )
				.queryParam( "length", 5 )
				.queryParam( "start", 0 )
				.queryParam( "searchText", "simpleSearchText=/csap/ui/*,appId=" + appIdLifeResourceCategory[0] +
						",lifecycle=" + appIdLifeResourceCategory[1] + from + to )
				.build( ).toUri( ) ;

		try {

			// latest metric event and then data
			String latestEventResponse = restTemplate.getForObject( eventUiUri, String.class ) ;

			logger.debug( "latest user events using {} \n {} ", eventUiUri, latestEventResponse ) ;

			if ( StringUtils.isNotBlank( latestEventResponse ) && ! latestEventResponse.equalsIgnoreCase( "null" ) ) {

				JsonNode eventJson = jacksonMapper.readTree( latestEventResponse ) ;
				JsonNode dataNode = eventJson.at( "/data" ) ;

				if ( dataNode.isArray( ) && dataNode.size( ) > 1 ) {

					logger.debug( "all good" ) ;

				} else {

					String reason = "Did not find data: " + eventUiUri ;
					userEventIssues.add( reason ) ;
					logger.error( reason ) ;

				}

			} else {

				logger.debug( "Latest event is null for url {} ", eventUiUri ) ;

			}

		} catch ( Exception e ) {

			userEventIssues.add( "Exception while running: " + eventUiUri + " Exception: " + e.getClass( )
					.getSimpleName( ) ) ;
			logger.error( "{} Failed to load: {}", eventUiUri, CSAP.buildCsapStack( e ) ) ;

		}

		return userEventIssues ;

	}

	List<String> queryLatestMetrics ( String[] appIdLifeResourceCategory ) {

		// Get the latest event information
		List<String> latestMetricsIssues = new ArrayList<>( ) ;
		
		logger.debug(  "port: {}, context: {}", csapInformation.getHttpPort( ) , csapInformation.getHttpContext( ) );

		URI latestEventUri = UriComponentsBuilder
				.fromHttpUrl( "http://localhost:" + csapInformation.getHttpPort( ) + csapInformation.getHttpContext( ) )
				.path( CsapEventsApplication.EVENT_API + "/latest" )
				.queryParam( "appId", appIdLifeResourceCategory[0] )
				.queryParam( "life", appIdLifeResourceCategory[1] )
				.queryParam( "category", appIdLifeResourceCategory[3] )
				.build( ).toUri( ) ;

		try {

			// latest metric event and then data
			String latestEventResponse = restTemplate.getForObject( latestEventUri, String.class ) ;
			logger.debug( "latestEvent: {} \n Response: {} ", latestEventUri, latestEventResponse ) ;

			if ( StringUtils.isNotBlank( latestEventResponse ) && ! latestEventResponse.equalsIgnoreCase( "null" ) ) {

				List<String> queryMetridIssues = queryMetricsData( appIdLifeResourceCategory[2], latestEventResponse ) ;
				latestMetricsIssues.addAll( queryMetridIssues ) ;

			} else {

				logger.debug( "Latest event is null for url {} ", latestEventUri ) ;

			}

		} catch ( Exception e ) {

			latestMetricsIssues.add( "Exception while running: " + latestEventUri + " Exception: " + e.getClass( )
					.getSimpleName( ) ) ;
			logger.error( "Exception from: {}, {}", latestEventUri, CSAP.buildCsapStack( e ) ) ;

		}

		return latestMetricsIssues ;

	}

	private List<String> queryMetricsData (
											String category ,
											String latestEventResponse )
		throws Exception {

		List<String> metricDataIssues = new ArrayList<>( ) ;
		JsonNode eventJson = jacksonMapper.readTree( latestEventResponse ) ;
		String host = eventJson.at( "/host" ).asText( ) ;
		String life = eventJson.at( "/lifecycle" ).asText( ) ;
		String appId = eventJson.at( "/appId" ).asText( ) ;
		logger.debug( "host {} life {} appId {} ", host, life, appId ) ;

		if ( StringUtils.isNotBlank( host ) && StringUtils.isNotBlank( appId ) && StringUtils.isNotBlank( life ) ) {

			URI urlWithParams = UriComponentsBuilder
					.fromHttpUrl( "http://localhost:" + csapInformation.getHttpPort( ) + csapInformation
							.getHttpContext( ) )
					.path( "/api/metrics/" + host + "/" + category )
					.queryParam( "appId", appId )
					.queryParam( "life", life )
					.queryParam( "numberOfDays", 1 )
					.queryParam( "padLatest", false )
					.build( ).toUri( ) ;

			logger.debug( "latest metrics url {} ", urlWithParams ) ;
			String metricsResponse = null ;

			try {

				metricsResponse = restTemplate.getForObject( urlWithParams, String.class ) ;

			} catch ( Exception e ) {

				logger.error( "Exception from: {}, {}", urlWithParams, CSAP.buildCsapStack( e ) ) ;
				throw e ;

			}

			if ( ( null != metricsResponse ) &&
					( metricsResponse.length( ) > 100 ) ) {

				logger.debug( "Healthy true" ) ;

			} else {

				metricDataIssues.add( "Failed to get metrics from category" + category ) ;
				logger.warn( "failed getting metrics for: {} , response: {} ", urlWithParams.toString( ),
						metricsResponse ) ;

			}

		}

		return metricDataIssues ;

	}

	private volatile List<String> failureReasons = new ArrayList<>( ) ;

	public List<String> getFailureReasons ( ) {

		return failureReasons ;

	}

	Random random = new Random( ) ;

	private String[] getAppIdLifeReportPattern ( ) {

		String target = getAppIds( ).get( random.nextInt( getAppIds( ).size( ) ) ) ;

		target += ":" + getLifes( ).get( random.nextInt( getLifes( ).size( ) ) ) ;

		target += ":" + testCategories[random.nextInt( testCategories.length )] ;

		return target.split( ":" ) ;

	}

	private String[] oldtestCategories = {
			"resource_30:/csap/metrics/host/30/data",
			"resource_300:/csap/metrics/host/300/data",
			"resource_3600:/csap/metrics/host/3600/data",
			"service_30:/csap/metrics/process/30/data",
			"service_300:/csap/metrics/process/300/data",
			"service_3600:/csap/metrics/process/3600/data",
			"jmx_30:/csap/metrics/jmx/standard/30/data",
			"jmx_300:/csap/metrics/jmx/standard/300/data",
			"jmx_3600:/csap/metrics/jmx/standard/3600/data"
	} ;

	private String[] testCategories = {
			"host_30:/csap/metrics/host/30/data",
			"host_300:/csap/metrics/host/300/data",
			"host_3600:/csap/metrics/host/3600/data",
			"os-process_30:/csap/metrics/os-process/30/data",
			"os-process_300:/csap/metrics/os-process/300/data",
			"os-process_3600:/csap/metrics/os-process/3600/data",
			"java_30:/csap/metrics/java/30/data",
			"java_300:/csap/metrics/java/300/data",
			"java_3600:/csap/metrics/java/3600/data"
	} ;

	public List<String> getAppIds ( ) {

		return appIds ;

	}

	public void setAppIds ( List<String> appIds ) {

		this.appIds = appIds ;

	}

	public List<String> getLifes ( ) {

		return lifes ;

	}

	public void setLifes ( List<String> lifes ) {

		this.lifes = lifes ;

	}

	public String getEventDataServiceContext ( ) {

		return eventDataServiceContext ;

	}

	public void setEventDataServiceContext ( String eventServiceContext ) {

		this.eventDataServiceContext = eventServiceContext ;

	}

	public String getEventAnalyticServiceContext ( ) {

		return eventAnalyticServiceContext ;

	}

	public void setEventAnalyticServiceContext ( String eventAnalyticServiceContext ) {

		this.eventAnalyticServiceContext = eventAnalyticServiceContext ;

	}

	public int getUserEventIdleDays ( ) {

		return userEventIdleDays ;

	}

	public void setUserEventIdleDays ( int userEventIdleDays ) {

		this.userEventIdleDays = userEventIdleDays ;

	}

}
