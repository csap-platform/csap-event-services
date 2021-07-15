package org.csap.test.reports ;

import static org.assertj.core.api.Assertions.assertThat ;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status ;

import java.util.ArrayList ;
import java.util.List ;
import java.util.Map ;

import javax.inject.Inject ;

import org.csap.events.EventJsonConstants ;
import org.csap.events.db.AnalyticsHelper ;
import org.csap.helpers.CSAP ;
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

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ArrayNode ;

@CsapEventsTests.MockTests
@Tag ( "mongo" )
public class ReportsApiTest {

	Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	@Autowired
	private MongoEmbedded mongoTest ;

	@Inject
	private ObjectMapper jacksonMapper ;

	@Test
	public void verify_vm_report_for_single_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String vmReport = retrieveAnalyticsReport( mockMvc, mongoTest.createStandardUrlParamMap( "1" ),
				"/api/report/vm" ) ;
		JsonNode vmReportNode = jacksonMapper.readTree( vmReport ) ;
		JsonNode dataElement = mongoTest.getJsonNodeFromArray( vmReportNode.at( "/data" ), "hostName",
				"netmet-desktop09" ) ;

		assertThat( dataElement.at( "/cpuCountAvg" ).asDouble( ) )
				.as( "CPU count avg does not match" )
				.isEqualTo( 16.0 ) ;

		assertThat( dataElement.at( "/totalUsrCpu" ).asLong( ) )
				.as( "Total usr cpu does not match" )
				.isEqualTo( 6468 ) ;

	}

	@Test
	public void verify_vm_report_multi_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String vmReport = retrieveAnalyticsReport( mockMvc, mongoTest.createStandardUrlParamMap( "2" ),
				"/api/report/vm" ) ;
		JsonNode vmReportNode = jacksonMapper.readTree( vmReport ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( vmReportNode ) ) ;

		JsonNode dataElement = mongoTest.getJsonNodeFromArray( vmReportNode.at( "/data" ), "hostName",
				"netmet-desktop09" ) ;
		logger.info( "multi" + dataElement ) ;

		assertThat( dataElement.at( "/cpuCountAvg" ).asDouble( ) )
				.as( "CPU count avg does not match" )
				.isEqualTo( 16.0 ) ;

		assertThat( dataElement.at( "/totalUsrCpu" ).asLong( ) )
				.as( "Total usr cpu does not match" )
				.isEqualTo( 24914 ) ;

	}

	@Test
	public void verify_service_report_multi_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String serviceReport = retrieveAnalyticsReport( mockMvc, mongoTest.createStandardUrlParamMap( "2" ),
				"/api/report/service" ) ;

		JsonNode serviceReportNode = jacksonMapper.readTree( serviceReport ) ;
		JsonNode dataElement = mongoTest.getJsonNodeFromArray( serviceReportNode.at( "/data" ), "serviceName",
				EventJsonConstants.CSAP_AGENT ) ;

		assertThat( dataElement.at( "/rssMemory" ).asLong( ) )
				.as( "Rss memory does not match" )
				.isEqualTo( 351774 ) ;

		assertThat( dataElement.at( "/fileCount" ).asLong( ) )
				.as( "File count does not match" )
				.isEqualTo( 48534 ) ;

	}

	@Test
	public void verify_service_report_with_host_and_service_filter_single_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "1" ) ;
		urlParams.put( "host", "csap-dev11" ) ;
		urlParams.put( "serviceName", "kubelet" ) ;
		String serviceReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/service" ) ;
		JsonNode serviceReportNode = jacksonMapper.readTree( serviceReport ) ;

		JsonNode dataElement = mongoTest.getJsonNodeFromArray( serviceReportNode.at( "/data" ), "serviceName",
				"kubelet" ) ;

		logger.debug( "service report node {} ", dataElement ) ;

		assertThat( dataElement.at( "/rssMemory" ).asLong( ) )
				.as( "Rss memory does not match" )
				.isEqualTo( 16673 ) ;

		assertThat( dataElement.at( "/fileCount" ).asLong( ) )
				.as( "File count does not match" )
				.isEqualTo( 5453 ) ;

	}

	@Test
	public void verify_service_details_report_multi_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "serviceName", "kubelet" ) ;
		String serviceReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/service/detail" ) ;
		JsonNode serviceReportNode = jacksonMapper.readTree( serviceReport ) ;
		JsonNode dataElement = mongoTest.getJsonNodeFromArray( serviceReportNode.at( "/data" ), "serviceName",
				"kubelet" ) ;
		logger.debug( "service report node {} ", dataElement ) ;

		assertThat( dataElement.at( "/rssMemory" ).asLong( ) )
				.as( "Rss memory does not match" )
				.isEqualTo( 33346 ) ;

		assertThat( dataElement.at( "/fileCount" ).asLong( ) )
				.as( "File count does not match" )
				.isEqualTo( 10906 ) ;

	}

	@Test
	public void verify_jmx_report_multi_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		var params = mongoTest.createStandardUrlParamMap( "2" ) ;
//		params.put( "category", "/csap/reports/jmxCustom/daily" ) ;

		String jmxReportResponse = retrieveAnalyticsReport( mockMvc, params, "/api/report/jmx" ) ;

		var javaReport = jacksonMapper.readTree( jmxReportResponse ) ;
		logger.info( "javaReport: {}", CSAP.jsonPrint( javaReport ) ) ;

		JsonNode dataElement = mongoTest.getJsonNodeFromArray( javaReport.at( "/data" ), "serviceName",
				EventJsonConstants.CSAP_AGENT ) ;

		logger.debug( "data element {} ", dataElement ) ;

		assertThat( dataElement.at( "/httpProcessingTime" ).asLong( ) )
				.as( "httpProcessingTime does not match" )
				.isEqualTo( 1513899 ) ;

		assertThat( dataElement.at( "/sessionsActive" ).asLong( ) )
				.as( "sessionsActive does not match" )
				.isEqualTo( 397 ) ;

	}

	@Test
	public void verify_jmx_details_report_multi_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "serviceName", EventJsonConstants.CSAP_AGENT ) ;
		String jmxDetailsReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/jmx/detail" ) ;
		JsonNode jmxDetailsReportNode = jacksonMapper.readTree( jmxDetailsReport ) ;
		JsonNode dataElement = mongoTest.getJsonNodeFromArray( jmxDetailsReportNode.at( "/data" ), "serviceName",
				EventJsonConstants.CSAP_AGENT ) ;
		logger.debug( "data element {} ", dataElement ) ;

		assertThat( dataElement.at( "/httpKbytesReceived" ).asLong( ) )
				.as( "httpKbytesReceived does not match" )
				.isEqualTo( 120 ) ;

		assertThat( dataElement.at( "/httpRequestCount" ).asLong( ) )
				.as( "httpRequestCount does not match" )
				.isEqualTo( 2704 ) ;

	}

	@Test
	public void verify_custom_jmx_report_multi_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "serviceName", EventJsonConstants.CSAP_AGENT ) ;
		String customJmxReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/jmxCustom" ) ;
		JsonNode customJmxReportNode = jacksonMapper.readTree( customJmxReport ) ;
		JsonNode dataElement = mongoTest.getJsonNodeFromArray( customJmxReportNode.at( "/data" ), "serviceName",
				EventJsonConstants.CSAP_AGENT ) ;
		logger.debug( "data element {} ", dataElement ) ;

		assertThat( dataElement.at( "/LogRotationMs" ).asLong( ) )
				.as( "LogRotationMs does not match" )
				.isEqualTo( 121829 ) ;

		assertThat( dataElement.at( "/TotalVmCpu" ).asLong( ) )
				.as( "TotalVmCpu does not match" )
				.isEqualTo( 51928 ) ;

	}

	@Test
	public void verify_custom_jmx_details_report_multi_day ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "serviceName", EventJsonConstants.CSAP_AGENT ) ;
		String jmxDetailsReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/jmxCustom/detail" ) ;
		JsonNode jmxDetailsReportNode = jacksonMapper.readTree( jmxDetailsReport ) ;
		JsonNode dataElement = mongoTest.getJsonNodeFromArray( jmxDetailsReportNode.at( "/data" ), "serviceName",
				EventJsonConstants.CSAP_AGENT ) ;
		logger.debug( "data element {} ", dataElement ) ;

		assertThat( dataElement.at( "/SpringMvcRequests" ).asLong( ) )
				.as( "SpringMvcRequests does not match" )
				.isEqualTo( 2703 ) ;

		assertThat( dataElement.at( "/TotalVmCpu" ).asLong( ) )
				.as( "TotalVmCpu does not match" )
				.isEqualTo( 25810 ) ;

	}

	@Test
	public void verify_service_attributes ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "serviceName", EventJsonConstants.CSAP_AGENT ) ;
		urlParams.put( "category", "/csap/reports/jmxCustom/daily" ) ;

		CSAP.setLogToDebug( AnalyticsHelper.class.getName( ) ) ;
		String attributesResponse = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/attributes" ) ;
		CSAP.setLogToInfo( AnalyticsHelper.class.getName( ) ) ;

		JsonNode attributesNode = jacksonMapper.readTree( attributesResponse ) ;

		assertThat( attributesNode.isArray( ) ).isTrue( ) ;
		ArrayNode attributes = (ArrayNode) attributesNode ;

		logger.info( "attributes: {}", attributes ) ;

		String expectedResponse = "[\"serviceName\",\"numberOfSamples\",\"TotalVmCpu\",\"ProcessCpu\",\"AdminPingsMeanMs\",\"linuxPsAndMatchMeanMs\",\"LogRotationMs\",\"serviceDiskUsage\",\"publishEvents\",\"publishEventsMs\",\"SpringMvcRequests\",\"JavaCollectionCounter\",\"JmxCollectionMs\",\"JmxCollectionFailures\",\"JmxCustomCollectionFailures\",\"CommandsSinceRestart\",\"OsCommandsCounter\",\"OsCommandsMeanMs\",\"OsCommandsMaxTimeMs\",\"jmxHeartbeatMs\"]" ;
		JsonNode expectedAttributesNode = jacksonMapper.readTree( expectedResponse ) ;
		ArrayNode expectedAttributes = (ArrayNode) expectedAttributesNode ;
		List<String> expectedAttributesList = new ArrayList<>( ) ;

		for ( JsonNode attr : expectedAttributes ) {

			expectedAttributesList.add( attr.asText( ) ) ;

		}

		logger.debug( "expectedAttributesList {} ", expectedAttributesList ) ;
		attributes.forEach( attribNode -> {

			assertThat( expectedAttributesList.contains( attribNode.asText( ) ) )
					.as( "Missing attribute " + attribNode.asText( ) )
					.isTrue( ) ;

		} ) ;

	}

	private String retrieveAnalyticsReport ( MockMvc mockMvc , Map<String, String> urlParams , String url )
		throws Exception {

		MockHttpServletRequestBuilder requestBuilder = post( url ) ;

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
