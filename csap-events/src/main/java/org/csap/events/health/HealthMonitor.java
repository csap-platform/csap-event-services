package org.csap.events.health ;

import java.time.LocalDate ;
import java.time.format.DateTimeFormatter ;
import java.util.ArrayList ;
import java.util.List ;
import java.util.Random ;
import java.util.concurrent.TimeUnit ;

import javax.annotation.PostConstruct ;
import javax.inject.Inject ;

import org.bson.Document ;
import org.csap.alerts.AlertProcessor ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.EventMetaData ;
import org.csap.events.db.EventDataReader ;
import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapRestTemplateFactory ;
import org.csap.integations.CsapMicroMeter ;
import org.csap.integations.CsapPerformance ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.boot.context.properties.ConfigurationProperties ;
import org.springframework.context.ApplicationContext ;
import org.springframework.context.annotation.Bean ;
import org.springframework.context.annotation.Configuration ;
import org.springframework.scheduling.annotation.Scheduled ;

import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.MongoClient ;

@Configuration
@ConfigurationProperties ( prefix = CsapEventsApplication.CONFIGURATION_PREFIX + ".health-monitoring" )
public class HealthMonitor {

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	private List<String> appIds = new ArrayList<>( ) ;
	private List<String> lifes = new ArrayList<>( ) ;

	@PostConstruct
	public void showConfiguration ( ) {

		logger.info( "checking health every minute using appids: {} and lifes: {}", getAppIds( ), getLifes( ) ) ;
		;

	}

	@Bean
	public CsapPerformance.CustomHealth dataHealth ( ) {

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

				return CsapEventsApplication.class.getName( ) ;

			}
		} ;

		return health ;

	}

	private volatile List<String> failureReasons = new ArrayList<>( ) ;

	private List<String> getFailureReasons ( ) {

		return failureReasons ;

	}

	@Autowired
	private ApplicationContext applicationContext ;

	@Inject
	CsapEventsApplication csapEventsApplication ;

	@Scheduled ( fixedRateString = "${csap-events.health-monitoring.intervals-ms.members:30000}" , initialDelayString = "${csap-events.health-monitoring.intervals-ms.members:30000}" )
	public void verify_mongo_cluster_members ( ) {

		List<String> latestIssues = new ArrayList<>( ) ;

		if ( ! csapEventsApplication.getMongoConfig( ).isStandAlone( ) ) {

			Document stats = applicationContext
					.getBean( MongoClient.class )
					.getDatabase( "admin" )
					.runCommand( new Document( "replSetGetStatus", 1 ) ) ;

			logger.debug( "replica status: {}", stats ) ;

			@SuppressWarnings ( "unchecked" )
			ArrayList<Document> dbList = (ArrayList<Document>) stats.get( "members" ) ;

			if ( null != dbList ) {

				for ( Document statusDoc : dbList ) {

					double healthRc = statusDoc.getDouble( "health" ) ;

					if ( healthRc != 1.0 ) {

						String host = statusDoc.getString( "name" ) ;
						String stateStr = statusDoc.getString( "stateStr" ) ;
						logger.warn( "Mongo instance health error: '{}' status: '{}' (1.0 is healthy). stateStr: '{}'",
								host, healthRc,
								stateStr ) ;
						latestIssues.add( "Mongo Instance: '" + statusDoc.getString( "name" ) + "', health error: '"
								+ healthRc
								+ "'; 1.0 indicates healthy. stateStr: " + stateStr ) ;

					}

				}

			} else {

				latestIssues.add( "Failed to get mongo replica status" ) ;

			}

		}

		failureReasons = latestIssues ;

	}

	Random random = new Random( ) ;
	@Inject
	EventDataReader eventDataReader ;

	@Scheduled ( fixedRateString = "${csap-events.health-monitoring.intervals-ms.search:60000}" , initialDelayString = "${csap-events.health-monitoring.intervals-ms.search:60000}" )
	public void verify_search_filters ( ) {

		List<String> latestIssues = new ArrayList<>( ) ;

		LocalDate today = LocalDate.now( ) ; // format(
												// DateTimeFormatter.ofPattern(
												// "HH:mm:ss, MMMM d uuuu " ) ) ;
		LocalDate weekAgo = LocalDate.now( ).minusDays( 7 ) ; // format(
																// DateTimeFormatter.ofPattern(
																// "HH:mm:ss, MMMM d
																// uuuu " ) ) ;

		try {

			String appId = getAppIds( ).get( random.nextInt( getAppIds( ).size( ) ) ) ;
			String life = getLifes( ).get( random.nextInt( getLifes( ).size( ) ) ) ;
			String fromDate = weekAgo.format( DateTimeFormatter.ofPattern( "M/d/yyyy" ) ) ;
			String toDate = today.format( DateTimeFormatter.ofPattern( "M/d/yyyy" ) ) ;

			var timer = metricUtilities.startTimer( ) ;

			logger.debug( "Testing: {}, {}, {}, {}", appId, life, fromDate, toDate ) ;
			EventMetaData searchFilters = eventDataReader.getEventMetaData(
					appId,
					life,
					fromDate, toDate, 60 ) ;
			var nanos = metricUtilities.stopTimer( timer, EventDataReader.SEARCH_FILTER_KEY + "health" ) ;

			logger.debug( "query: {}, {}, {}, {}, Time: {} searchFilters: \n {}",
					CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ), appId, life, fromDate, toDate ) ;

			if ( searchFilters.getAppIds( ).isEmpty( ) || searchFilters.getUiUsers( ).isEmpty( ) ) {

				latestIssues.add( EventDataReader.SEARCH_FILTER_KEY + " - Failed to find users or appids" ) ;

			}

		} catch ( Exception e ) {

			latestIssues.add( EventDataReader.SEARCH_FILTER_KEY + " - Failed to get resutls" + e.getMessage( ) ) ;
			logger.error( "Failed to load: {}", CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) ) ;

		}

		failureReasons = latestIssues ;

	}

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

}
