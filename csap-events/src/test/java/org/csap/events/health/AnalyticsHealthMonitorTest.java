package org.csap.events.health ;

import static org.assertj.core.api.Assertions.assertThat ;
import static org.mockito.ArgumentMatchers.any ;
import static org.mockito.Mockito.doAnswer ;
import static org.mockito.Mockito.doReturn ;
import static org.mockito.Mockito.spy ;
import static org.mockito.Mockito.when ;

import java.text.SimpleDateFormat ;
import java.util.Arrays ;
import java.util.Collections ;
import java.util.Date ;
import java.util.List ;
import java.util.concurrent.TimeUnit ;

import org.csap.alerts.AlertProcessor ;
import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapApplication ;
import org.csap.integations.CsapInformation ;
import org.csap.integations.CsapMicroMeter ;
import org.junit.jupiter.api.BeforeAll ;
import org.junit.jupiter.api.BeforeEach ;
import org.junit.jupiter.api.Test ;
import org.junit.jupiter.api.TestInstance ;
import org.mockito.stubbing.Answer ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration ;
import org.springframework.boot.test.context.SpringBootTest ;
import org.springframework.boot.test.mock.mockito.MockBean ;
import org.springframework.test.context.ActiveProfiles ;
import org.springframework.web.client.RestTemplate ;

@SpringBootTest ( classes = {
		AnalyticsHealthMonitor.class, CsapMicroMeter.class, JacksonAutoConfiguration.class
} )
@ActiveProfiles ( "junit" )
@TestInstance ( TestInstance.Lifecycle.PER_CLASS )
public class AnalyticsHealthMonitorTest {

	Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	private static final String NULL_RESP = "null" ;
	private static final String USER_EVENT_PASSES = "{\"data\": [\"test1\", \"test2\"]}" ;
	private static final String USER_EVENT_FAILS = "{\"data\": null}" ;
	private static final String LATEST_METRICS_PASSES = "{\"host\": \"myHost\", \"lifecycle\": \"myLife\", \"appId\": \"myAppId\" }" ;
	private static final String METRIC_RESPONSE_PASSES = "something more than 100 chars. blah, blah, blah, blah, blah, blah,"
			+ " blah, blah, blah, blah, blah, blah, blah, blah, blah, blah" ;
	private static final String METRIC_RESPONSE_FAILS = "something less than 100 chars" ;
	private static final String[] APP_ID_LIFE_RES_CAT = {
			"test1", "test2", "test3", "test4"
	} ;
	private static final String LATEST_METRIC = "latestMetric" ;
	private static final String LATEST_USER = "latestUser" ;
	private static final String USER_ANALYTICS = "UserAnalytics" ;
	private static final String LB_URL = "http://fake.net" ;

	@Autowired
	private AnalyticsHealthMonitor healthMonitor ;

	@MockBean
	private RestTemplate template ;

	@MockBean
	private CsapInformation csapInformation ;

	@MockBean
	private AlertProcessor alertProcessor ;

	@BeforeEach
	void beforeAll ( )
		throws Exception {

		when( csapInformation.getLoadBalancerUrl( ) ).thenReturn( LB_URL ) ;
		when( csapInformation.getHttpPort( ) ).thenReturn( "8080" ) ;
		when( csapInformation.getHttpContext( ) ).thenReturn( "/csap-events" ) ;

	}

	private static String getDay ( int offsetDays ) {

		SimpleDateFormat formatter = new SimpleDateFormat( "MM/dd/yyyy" ) ;
		Date offsetDate = new Date( System.currentTimeMillis( ) - TimeUnit.DAYS.toMillis( offsetDays ) ) ;
		return formatter.format( offsetDate ) ;

	}

	private static String generateTo ( ) {

		return "to=" + getDay( 0 ) ;

	}

	private static String generateFrom ( int offsetDays ) {

		return "from=" + getDay( offsetDays ) ;

	}

	@Test
	public void queryLatestUserEventsPasses ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		when( template.getForObject( any( ), any( ) ) ).thenReturn( USER_EVENT_PASSES ) ;
		

		// when
		List<String> latestIssues = healthMonitor.queryLatestUserEvents( APP_ID_LIFE_RES_CAT ) ;

		// then
		assertThat( latestIssues ).isEmpty( ) ;

	}

	@Test
	public void queryLatestUserEventsFails ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		when( template.getForObject( any( ), any( ) ) ).thenReturn( USER_EVENT_FAILS ) ;
		

		// when
		List<String> latestIssues = healthMonitor.queryLatestUserEvents( APP_ID_LIFE_RES_CAT ) ;

		// then
		// has one entry that has an error of "Did not find data ..."
		assertThat( latestIssues.size( ) ).isEqualTo( 1 ) ;
		assertThat( latestIssues.get( 0 ) ).contains( "Did not find data" ) ;

	}

	@Test
	public void queryLatestUserEventsException ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		when( template.getForObject( any( ), any( ) ) ).thenThrow( new IllegalStateException( ) ) ;
		

		// when
		List<String> latestIssues = healthMonitor.queryLatestUserEvents( APP_ID_LIFE_RES_CAT ) ;

		// then
		// has one entry that has an error of "Did not find data ..."
		assertThat( latestIssues.size( ) ).isEqualTo( 1 ) ;
		assertThat( latestIssues.get( 0 ) ).contains( "Exception while running" ) ;

	}

	@Test
	public void queryLatestUserEventsTurnOff ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		// Note: not getForObject() is called.
		healthMonitor.setUserEventIdleDays( 0 ) ;

		// when
		List<String> latestIssues = healthMonitor.queryLatestUserEvents( APP_ID_LIFE_RES_CAT ) ;

		// then
		assertThat( latestIssues ).isEmpty( ) ;
		;

	}

	@Test
	public void queryLatestMetricsNull ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		when( template.getForObject( any( ), any( ) ) ).thenReturn( NULL_RESP ) ;

		// when
		CSAP.setLogToDebug( healthMonitor.getClass( ).getName( ) ) ;
		List<String> latestIssues = healthMonitor.queryLatestMetrics( APP_ID_LIFE_RES_CAT ) ;

		logger.info( "latestIssues: {}", latestIssues ) ;
		// then
		assertThat( latestIssues ).isEmpty( ) ;

	}

	@Test
	public void queryLatestMetricsException ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		when( template.getForObject( any( ), any( ) ) ).thenThrow( new IllegalStateException( ) ) ;
		

		// when
		List<String> latestIssues = healthMonitor.queryLatestMetrics( APP_ID_LIFE_RES_CAT ) ;

		// then
		assertThat( latestIssues.size( ) ).isEqualTo( 1 ) ;
		assertThat( latestIssues.get( 0 ) ).contains( "Exception while running" ) ;

	}

	@Test
	public void queryLatestMetricsPasses ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		when( template.getForObject( any( ), any( ) ) ).thenReturn( LATEST_METRICS_PASSES )
				.thenReturn( METRIC_RESPONSE_PASSES ) ;
		

		// when
		List<String> latestIssues = healthMonitor.queryLatestMetrics( APP_ID_LIFE_RES_CAT ) ;

		// then
		assertThat( latestIssues ).isEmpty( ) ;

	}

	@Test
	public void queryLatestMetricsFails ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		when( template.getForObject( any( ), any( ) ) ).thenReturn( LATEST_METRICS_PASSES )
				.thenReturn( METRIC_RESPONSE_FAILS ) ;
		

		// when
		List<String> latestIssues = healthMonitor.queryLatestMetrics( APP_ID_LIFE_RES_CAT ) ;

		// then
		assertThat( latestIssues.size( ) ).isEqualTo( 1 ) ;
		assertThat( latestIssues.get( 0 ) ).contains( "Failed to get metrics" ) ;

	}

	@Test
	public void queryLatestMetricsResponseException ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		when( template.getForObject( any( ), any( ) ) ).thenReturn( LATEST_METRICS_PASSES )
				.thenThrow( new IllegalStateException( ) ) ;
		

		// when
		List<String> latestIssues = healthMonitor.queryLatestMetrics( APP_ID_LIFE_RES_CAT ) ;

		// then
		assertThat( latestIssues.size( ) ).isEqualTo( 1 ) ;
		assertThat( latestIssues.get( 0 ) ).contains( "Exception while running" ) ;

	}

	@Test
	public void monitorMetrics ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		// given
		// create a spy of our healthMonitor instance so we can override the query
		// methods.
		AnalyticsHealthMonitor spyHealthMonitor = spy( healthMonitor ) ;
		// override query methods (note: need to "doReturn")
		doReturn( Collections.singletonList( LATEST_METRIC ) ).when( spyHealthMonitor ).queryLatestMetrics( any(
				String[].class ) ) ;
		doReturn( Collections.singletonList( LATEST_USER ) ).when( spyHealthMonitor ).queryLatestUserEvents( any(
				String[].class ) ) ;
		doReturn( Collections.singletonList( USER_ANALYTICS ) ).when( spyHealthMonitor ).queryUserAnalytics( any(
				String[].class ) ) ;
		
		spyHealthMonitor.setAppIds( Arrays.asList( "test" ) ) ;
		spyHealthMonitor.setLifes( Arrays.asList( "test" ) ) ;

		// when
		spyHealthMonitor.monitorMetrics( ) ;

		// then
		assertThat( spyHealthMonitor.getFailureReasons( ).size( ) ).isEqualTo( 3 ) ;
		assertThat( spyHealthMonitor.getFailureReasons( ) ).containsExactly( LATEST_METRIC, LATEST_USER,
				USER_ANALYTICS ) ;

	}

	@Test
	public void verifyLatestEventsWindow ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		final int DAYS = 5 ;

		StringBuffer urlBuffer = new StringBuffer( ) ;

		doAnswer( (Answer<String>) invocation -> {

			urlBuffer.append( invocation.getArgument( 0 ).toString( ) ) ;
			return USER_EVENT_PASSES ;

		} ).when( template ).getForObject( any( ), any( ) ) ;

		

		// when
		healthMonitor.setUserEventIdleDays( DAYS ) ;
		List<String> latestIssues = healthMonitor.queryLatestUserEvents( APP_ID_LIFE_RES_CAT ) ;

		// then
		assertThat( latestIssues ).isEmpty( ) ;

		assertThat( urlBuffer.toString( ) ).contains( generateTo( ) ) ;
		assertThat( urlBuffer.toString( ) ).contains( generateFrom( DAYS ) ) ;

	}
}
