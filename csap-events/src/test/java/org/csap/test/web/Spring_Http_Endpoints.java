package org.csap.test.web ;

import static org.assertj.core.api.Assertions.assertThat ;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status ;

import org.csap.events.CsapEventsApplication ;
import org.csap.helpers.CsapApplication ;
import org.junit.jupiter.api.Test ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc ;
import org.springframework.boot.test.context.SpringBootTest ;
import org.springframework.http.MediaType ;
import org.springframework.scheduling.annotation.EnableScheduling ;
import org.springframework.test.context.ActiveProfiles ;
import org.springframework.test.context.web.WebAppConfiguration ;
import org.springframework.test.web.servlet.MockMvc ;
import org.springframework.test.web.servlet.ResultActions ;

@WebAppConfiguration
@SpringBootTest ( classes = {
		CsapEventsApplication.class
} )
@EnableScheduling ( )
@AutoConfigureMockMvc
@ActiveProfiles ( "junit" )
public class Spring_Http_Endpoints {
	final static private Logger logger = LoggerFactory.getLogger( Spring_Http_Endpoints.class ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	@Test
	public void http_get_analytics_landing_page ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		// mock does much validation.....
		ResultActions resultActions = mockMvc.perform(
				get( "/analytics/" )
						.param( "sampleParam1", "sampleValue1" )
						.param( "sampleParam2", "sampleValue2" )
						.accept( MediaType.TEXT_PLAIN ) ) ;

		//
		String result = resultActions
				.andExpect( status( ).isOk( ) )
				.andExpect( content( ).contentType( "text/html;charset=UTF-8" ) )
				.andReturn( ).getResponse( ).getContentAsString( ) ;
		logger.info( "result:\n" + result ) ;

		assertThat( result )
				.contains( "CSAP Adoption" ) ;

	}

	@Test
	public void http_get_data_landing_page ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		// mock does much validation.....
		ResultActions resultActions = mockMvc.perform( get( "/" ).param( "sampleParam1", "sampleValue1" )
				.param( "sampleParam2", "sampleValue2" ).accept( MediaType.TEXT_PLAIN ) ) ;

		//
		String result = resultActions.andExpect( status( ).isOk( ) )
				.andExpect( content( ).contentType( "text/html;charset=UTF-8" ) ).andReturn( ).getResponse( )
				.getContentAsString( ) ;
		logger.info( "result:\n" + result ) ;

		assertThat( result ).contains( "Events Viewer" ) ;

	}

}
