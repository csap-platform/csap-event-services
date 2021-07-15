package org.csap.test.reports ;

import static org.assertj.core.api.Assertions.assertThat ;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status ;

import java.util.Map ;

import javax.inject.Inject ;

import org.csap.events.CsapEventsApplication ;
import org.csap.events.EventJsonConstants ;
import org.csap.helpers.CsapApplication ;
import org.csap.test.container.CsapEventsTests ;
import org.junit.jupiter.api.Tag ;
import org.junit.jupiter.api.Test ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.http.MediaType ;
import org.springframework.test.web.servlet.MockMvc ;
import org.springframework.test.web.servlet.ResultActions ;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder ;

import com.fasterxml.jackson.databind.ObjectMapper ;

@CsapEventsTests.MockTests
@Tag ( "mongo" )
public class TopLowHostsTest {

	private static Logger logger = LoggerFactory.getLogger( TopLowHostsTest.class ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	@Autowired
	private MongoEmbedded mongoTest ;

	@Inject
	private ObjectMapper jacksonMapper ;

	@Test
	public void verify_top_by_unHealthy_count ( @Autowired MockMvc mockMvc )
		throws Exception {

		String responseText = retrieveTopHosts( mockMvc, "1", "health.UnhealthyEventCount", "1" ) ;

		assertThat( responseText )
				.as( "Host name does not match" )
				.isEqualTo( "[\"centos1\"]" ) ;

	}

	@Test
	public void verify_top_by_custom_jmx_data ( @Autowired MockMvc mockMvc )
		throws Exception {

		String responseText = retrieveTopHosts( mockMvc, "1",
				"jmxCustom."
						+ EventJsonConstants.CSAP_AGENT
						+ ".TotalVmCpu", "1" ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( responseText ) ) ;

		assertThat( responseText )
				.as( "Host name does not match" )
				.isEqualTo( "[\"netmet-perf02\"]" ) ;

	}

	@Test
	public void verify_top_by_jmx_by_service ( @Autowired MockMvc mockMvc )
		throws Exception {

		String responseText = retrieveTopHosts( mockMvc, "10",
				"jmx.HeapUsed_"
						+ EventJsonConstants.CSAP_AGENT, "1" ) ;

		assertThat( responseText )
				.as( "Host name does not match" )
				.isEqualTo( "[\"netmet-perf02\"]" ) ;

	}

	@Test
	public void verify_top_by_jmx_by_heap ( @Autowired MockMvc mockMvc )
		throws Exception {

		String responseText = retrieveTopHosts( mockMvc, "10", "jmx.HeapUsed", "2" ) ;

		assertThat( responseText )
				.as( "Host name does not match" )
				.isEqualTo( "[\"netmet-perf02\",\"csap-dev11\"]" ) ;

	}

	@Test
	public void verify_top_by_process_by_cpu ( @Autowired MockMvc mockMvc )
		throws Exception {

		String responseText = retrieveTopHosts( mockMvc, "10", "process.topCpu", "2" ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( responseText ) ) ;

		assertThat( responseText )
				.as( "Host name does not match" )
				.isEqualTo( "[\"csap-dev11\"]" ) ;

	}

	@Test
	public void verify_top_hosts_by_cores_active ( @Autowired MockMvc mockMvc )
		throws Exception {

		String responseText = retrieveTopHosts( mockMvc, "1", "vm.coresActive", "2" ) ;

		assertThat( responseText )
				.as( "Host name does not match" )
				.isEqualTo( "[\"netmet-desktop09\",\"csap-dev10\"]" ) ;

	}

	@Test
	public void verify_top_by_process_by_service_name ( @Autowired MockMvc mockMvc )
		throws Exception {

		String responseText = retrieveTopHosts( mockMvc, "1", "process.topCpu_docker", "1" ) ;

		assertThat( responseText )
				.as( "Host name does not match" )
				.isEqualTo( "[\"csap-dev11\"]" ) ;

	}

	@Test
	public void verify_low_hosts_by_cores_active ( @Autowired MockMvc mockMvc )
		throws Exception {

		String responseText = retrieveLowHosts( mockMvc, "1", "vm.coresActive", "2" ) ;

		assertThat( responseText )
				.as( "Host name does not match" )
				.isEqualTo( "[\"netmet-desktop09\",\"csap-dev10\"]" ) ;

	}

	private String retrieveTopHosts ( @Autowired MockMvc mockMvc , String numDays , String metricsId , String numHosts )
		throws Exception {

		return retrieveHosts( mockMvc, CsapEventsApplication.REPORT_API + "/top",
				numDays, metricsId, numHosts ) ;

	}

	private String retrieveLowHosts ( @Autowired MockMvc mockMvc , String numDays , String metricsId , String numHosts )
		throws Exception {

		return retrieveHosts( mockMvc, CsapEventsApplication.REPORT_API + "/low",
				numDays, metricsId, numHosts ) ;

	}

	private String retrieveHosts ( MockMvc mockMvc , String url , String numDays , String metricsId , String numHosts )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( numDays ) ;
		urlParams.put( "hosts", numHosts ) ;
		urlParams.put( "metricsId", metricsId ) ;

		MockHttpServletRequestBuilder requestBuilder = post( CsapEventsApplication.REPORT_API + "/top" ) ;

		for ( String key : urlParams.keySet( ) ) {

			requestBuilder.param( key, urlParams.get( key ) ) ;

		}

		requestBuilder.contentType( MediaType.APPLICATION_FORM_URLENCODED ) ;
		requestBuilder.accept( MediaType.APPLICATION_JSON ) ;
		ResultActions resultActions = mockMvc.perform( requestBuilder ) ;

		String responseText = resultActions.andExpect( status( ).isOk( ) )
				.andExpect( content( ).contentType( MediaType.APPLICATION_JSON ) )
				.andReturn( )
				.getResponse( )
				.getContentAsString( ) ;
		return responseText ;

	}
}
