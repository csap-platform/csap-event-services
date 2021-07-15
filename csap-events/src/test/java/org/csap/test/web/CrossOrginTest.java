package org.csap.test.web ;

import static org.assertj.core.api.Assertions.assertThat ;
import static org.mockito.ArgumentMatchers.any ;
import static org.mockito.ArgumentMatchers.anyBoolean ;
import static org.mockito.ArgumentMatchers.anyInt ;
import static org.mockito.Mockito.when ;

import java.net.URI ;
import java.net.URISyntaxException ;

import org.csap.events.CsapEventsApplication ;
import org.csap.events.db.EventDataReader ;
import org.csap.events.db.EventDataWriter ;
import org.csap.events.db.MetricsDataHandler ;
import org.csap.events.health.HealthMonitor ;
import org.csap.helpers.CsapApplication ;
import org.junit.jupiter.api.BeforeAll ;
import org.junit.jupiter.api.Test ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.boot.test.context.SpringBootTest ;
import org.springframework.boot.test.mock.mockito.MockBean ;
import org.springframework.boot.test.web.client.TestRestTemplate ;
import org.springframework.boot.web.server.LocalServerPort ;
import org.springframework.http.HttpStatus ;
import org.springframework.http.MediaType ;
import org.springframework.http.RequestEntity ;
import org.springframework.http.ResponseEntity ;
import org.springframework.test.context.ActiveProfiles ;

@SpringBootTest ( classes = {
		CsapEventsApplication.class
} , webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT )
@ActiveProfiles ( "junit" )
public class CrossOrginTest {
	final static private Logger logger = LoggerFactory.getLogger( CrossOrginTest.class ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	final static private String CALLBACK = "callback_name" ;
	final static private String DATA = "{\"test\": \"data\"}" ;
	final static private long COUNT = 12345L ;
	final static private String EVENT_COUNT_RES = "{\"count\":" + Long.toString( COUNT ) + "}" ;
	final static private String EVENT_API_DELETE = "{\"result\": \"Deleted " + COUNT + " record \"}" ;

	private static String convertToJsonpCallback ( String data , boolean jsonp ) {

		return ( jsonp ? CALLBACK + "(" : "" ) + data + ( jsonp ? ")" : "" ) ;

	}

	private static String metricsConvertToJsonpCallback ( String data , boolean jsonp ) {

		// Note: /api/metrics/ call wraps result in a {}
		// return (jsonp ? CALLBACK + "(" : "") + "{" + DATA + "}" + (jsonp ? ")" : "")
		// ;
		return ( jsonp ? CALLBACK + "(" : "" ) + DATA + ( jsonp ? ")" : "" ) ;

	}

	@LocalServerPort
	private int port ;

	@MockBean
	private EventDataReader mockEventsReader ;
	@MockBean
	private HealthMonitor mockHealthMonitor ;
	@MockBean
	private EventDataWriter mockEventDataWriter ;
	@MockBean
	private MetricsDataHandler mockMetricsHandler ;

	@BeforeAll
	public static void setUpBeforeClass ( )
		throws Exception {

		CsapApplication.initialize( logger.getName( ) ) ;

	}

	@Test
	public void http_test_cors_simple_api_event ( )
		throws Exception {

		logger.info( "test_cors" ) ;
		// mock does much validation.....
		when( mockEventsReader.countEvents( anyInt( ), any( ), any( ), any( ), any( ) ) ).thenReturn( COUNT ) ;

		testGetResponse( "/api/event/count?appId=SensusCsap",
				convertToJsonpCallback( EVENT_COUNT_RES, false ),
				"application/json" ) ;

	}

	@Test
	public void http_test_cors_options_api_event ( )
		throws Exception {

		logger.info( "test_cors" ) ;
		// mock does much validation.....
		when( mockEventsReader.countEvents( anyInt( ), any( ), any( ), any( ), any( ) ) ).thenReturn( COUNT ) ;

		testOptionResponse( "/api/event/count?appId=SensusCsap" ) ;

	}

	@Test
	public void http_test_cors_simple_metrics ( )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		// mock does much validation.....
		when( mockMetricsHandler.buildPerformanceGraphDataForToday( any( ), any( ), any( ), anyInt( ), anyInt( ),
				anyInt( ),
				anyInt( ), any( ), any( ), any( ), anyBoolean( ), anyBoolean( ) ) ).thenReturn( DATA ) ;

		testGetResponse( "/api/metrics/csap-dev02.lab.sensus.net/service_300?numberofDays=1",
				metricsConvertToJsonpCallback( DATA, false ),
				"application/json;charset=UTF-8" ) ;

	}

	@Test
	public void http_test_cors_options ( )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		testOptionResponse( "/api/metrics/csap-dev02.lab.sensus.net/service_300?numberofDays=1" ) ;

	}

	private void testGetResponse ( String restCall , String expectedResult , String expectedContentType )
		throws URISyntaxException {

		URI uri = new URI( "http://localhost:" + port + restCall ) ;

		TestRestTemplate restTemplate = new TestRestTemplate( ) ;
		RequestEntity<Void> request = RequestEntity.get( uri ).accept( MediaType.APPLICATION_JSON )
				.header( "Origin", "http://www.someurl.com" ).build( ) ;
		ResponseEntity<String> response = restTemplate.exchange( request, String.class ) ;

		assertThat( response.getStatusCode( ) ).isEqualTo( HttpStatus.OK ) ;
		assertThat( response.getHeaders( ).getContentType( ).toString( ) ).isEqualTo( expectedContentType ) ;

		logger.info( "***** expected result: {}", expectedResult ) ;

		assertThat( response.getBody( ) ).isEqualTo( expectedResult ) ;

	}

	private void testOptionResponse ( String restCall )
		throws URISyntaxException {

		URI uri = new URI( "http://localhost:" + port + restCall ) ;

		TestRestTemplate restTemplate = new TestRestTemplate( ) ;
		RequestEntity<Void> request = RequestEntity.options( uri ).accept( MediaType.TEXT_PLAIN )
				.header( "Origin", "http://www.someurl.com" )
				.header( "Access-Control-Request-Method", "GET" ).build( ) ;
		ResponseEntity<String> response = restTemplate.exchange( request, String.class ) ;

		assertThat( response.getStatusCode( ) ).isEqualTo( HttpStatus.OK ) ;
		assertThat( response.getHeaders( ).get( "Access-Control-Allow-Origin" ).get( 0 ) ).isEqualTo( "*" ) ;
		assertThat( response.getHeaders( ).get( "Access-Control-Allow-Methods" ).get( 0 ) ).isEqualTo( "GET" ) ;

	}

}
