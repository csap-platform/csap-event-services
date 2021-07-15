package org.csap.test.web ;

import static org.assertj.core.api.Assertions.assertThat ;

import java.net.URI ;

import javax.inject.Inject ;

import org.csap.events.CsapEventsApplication ;
import org.csap.helpers.CsapApplication ;
import org.junit.jupiter.api.Test ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.boot.test.context.SpringBootTest ;
import org.springframework.boot.test.web.client.TestRestTemplate ;
import org.springframework.boot.web.client.RestTemplateBuilder ;
import org.springframework.boot.web.server.LocalServerPort ;
import org.springframework.http.ResponseEntity ;
import org.springframework.test.context.ActiveProfiles ;

import com.fasterxml.jackson.databind.ObjectMapper ;

@SpringBootTest ( classes = {
		CsapEventsApplication.class
} , webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT )
// @WebIntegrationTest
@ActiveProfiles ( "junit" )
public class Performance_Console {
	final static private Logger logger = LoggerFactory.getLogger( Performance_Console.class ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	@LocalServerPort
	private int port ;

	ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	@Inject
	RestTemplateBuilder restTemplateBuilder ;

	@Test
	public void validate_csap_health_using_rest_template ( )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		// mock does much validation.....

		TestRestTemplate restTemplate = new TestRestTemplate( restTemplateBuilder ) ;

		URI uri = new URI( "http://localhost:" + port + "/health" ) ;

		ResponseEntity<String> response = restTemplate.getForEntity( uri, String.class ) ;

		logger.info( "result:\n" + response ) ;

		assertThat( response.getBody( ) )
				.contains( "table class=\"bordered iblock\" id=\"healthTable\"" ) ;

	}
}
