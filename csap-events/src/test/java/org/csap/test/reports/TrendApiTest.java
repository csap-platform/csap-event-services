package org.csap.test.reports ;

import static org.assertj.core.api.Assertions.assertThat ;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status ;

import java.util.List ;
import java.util.Map ;

import org.csap.events.EventJsonConstants ;
import org.csap.events.db.AnalyticsDbReader ;
import org.csap.events.db.TrendingReportHelper ;
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

import com.fasterxml.jackson.core.type.TypeReference ;
import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;

@CsapEventsTests.MockTests
@Tag ( "mongo" )
public class TrendApiTest {
	private static Logger logger = LoggerFactory.getLogger( TopLowHostsTest.class ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	// have to get the reportLogger because based on the level it returns different
	// results.
	private static Logger reportLogger = LoggerFactory.getLogger( AnalyticsDbReader.class ) ;

	@Autowired
	private MongoEmbedded mongoTest ;

	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	public ObjectMapper getJacksonMapper ( ) {

		return jacksonMapper ;

	}

	@Test
	public void verify_log_rotate_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "1" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "MeanSeconds" ) ;
		String logRotateReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/custom/logRotate" ) ;
		JsonNode logRotateReportNode = getJacksonMapper( ).readTree( logRotateReport ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( logRotateReportNode ) ) ;

		String expectedReport = "[{\"date\":[\"2019-02-01\"],\"MeanSeconds\":[0],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( logRotateReportNode.at( "/data" ) )
				.as( "Log rotate report does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_log_rotate_report_Filtered_By_Service_Name ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "MeanSeconds" ) ;
		urlParams.put( "serviceName", EventJsonConstants.CSAP_AGENT ) ;
		String logRotateReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/custom/logRotate" ) ;
		JsonNode logRotateReportNode = getJacksonMapper( ).readTree( logRotateReport ) ;
		logger.debug( "$$$$$$$ {} ", logRotateReport ) ;
		String expectedReport = "[{\"date\":[\"2019-02-01\"],\"MeanSeconds\":[0],\"serviceName\":\""
				+ EventJsonConstants.CSAP_AGENT
				+ "\",\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( logRotateReportNode.at( "/data" ) )
				.as( "Log rotate report does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_userid_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		String useridReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/userid" ) ;
		JsonNode userIdReportNode = getJacksonMapper( ).readTree( useridReport ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( userIdReportNode ) ) ;

		String expectedReport = "[{\"date\":[\"2019-01-31\"],\"totActivity\":[3],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( userIdReportNode.at( "/data" ) )
				.as( "userid report does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_vm_core_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		String vmCoreReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/custom/core" ) ;
		JsonNode vmCoreReportNode = getJacksonMapper( ).readTree( vmCoreReport ) ;
		logger.debug( "$$$$$$$ {} ", vmCoreReport ) ;

		String expectedReport ;

		if ( reportLogger.isDebugEnabled( ) ) {

			expectedReport = "[{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"totalSysCpu\":[8808,3249],\"totalUsrCpu\":[18446,6785],\"cpuCountAvg\":[16,32],\"coresUsed\":[1.6517575757575758,0.9930255009107469],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;

		} else {

			expectedReport = "[{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"coresUsed\":[1.6517575757575758,0.9930255009107469],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;

		}

		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( vmCoreReportNode.at( "/data" ) )
				.as( "vm core report does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_vm_core_per_vm_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "perVm", "true" ) ;
		String vmCoreReportPerVm = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/custom/core" ) ;
		JsonNode vmCoreReportPerVmNode = getJacksonMapper( ).readTree( vmCoreReportPerVm ) ;
		logger.debug( "$$$$$$$ {} ", vmCoreReportPerVm ) ;
		String expectedReport ;

		if ( reportLogger.isDebugEnabled( ) ) {

			expectedReport = "[{\"date\":[\"2019-02-01\"],\"totalSysCpu\":[215],\"totalUsrCpu\":[317],\"cpuCountAvg\":[16],\"coresUsed\":[0.46513661202185796],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"csap-dev10\"},{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"totalSysCpu\":[8808,3034],\"totalUsrCpu\":[18446,6468],\"cpuCountAvg\":[16,16],\"coresUsed\":[1.6517575757575758,0.527888888888889],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"netmet-desktop09\"}]" ;

		} else {

			expectedReport = "[{\"date\":[\"2019-02-01\"],\"coresUsed\":[0.46513661202185796],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"csap-dev10\"},{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"coresUsed\":[1.6517575757575758,0.527888888888889],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"netmet-desktop09\"}]" ;

		}

		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( vmCoreReportPerVmNode.at( "/data" ) )
				.as( "vm core report PER VM does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_vm_core_per_vm_report_top_2_hosts ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "perVm", "true" ) ;
		urlParams.put( "top", "2" ) ;
		String vmCoreReportPerVm = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/custom/core" ) ;
		JsonNode vmCoreReportPerVmNode = getJacksonMapper( ).readTree( vmCoreReportPerVm ) ;
		logger.debug( "$$$$$$$ {} ", vmCoreReportPerVm ) ;
		String expectedReport ;

		if ( reportLogger.isDebugEnabled( ) ) {

			expectedReport = "[{\"date\":[\"2019-02-01\"],\"totalSysCpu\":[215],\"totalUsrCpu\":[317],\"cpuCountAvg\":[16],\"coresUsed\":[0.46513661202185796],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"csap-dev10\"},{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"totalSysCpu\":[8808,3034],\"totalUsrCpu\":[18446,6468],\"cpuCountAvg\":[16,16],\"coresUsed\":[1.6517575757575758,0.527888888888889],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"netmet-desktop09\"}]" ;

		} else {

			expectedReport = "[{\"date\":[\"2019-02-01\"],\"coresUsed\":[0.46513661202185796],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"csap-dev10\"},{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"coresUsed\":[1.6517575757575758,0.527888888888889],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"netmet-desktop09\"}]" ;

		}

		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( vmCoreReportPerVmNode.at( "/data" ) )
				.as( "vm core report top 2 hosts does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_health_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		String healthReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/custom/health" ) ;
		JsonNode healthReportNode = getJacksonMapper( ).readTree( healthReport ) ;
		logger.debug( "$$$$$$$ {} ", healthReport ) ;
		String expectedReport = "[{\"date\":[\"2019-02-01\"],\"UnHealthyCount\":[69],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( healthReportNode.at( "/data" ) )
				.as( "health report does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_health_report_per_vm ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "perVm", "true" ) ;
		String healthReportPerVm = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/custom/health" ) ;
		JsonNode healthReportPerVmNode = getJacksonMapper( ).readTree( healthReportPerVm ) ;
		logger.debug( "$$$$$$$ {} ", healthReportPerVm ) ;
		String expectedReport = "[{\"date\":[\"2019-02-01\"],\"UnHealthyCount\":[69],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\",\"host\":\"centos1\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( healthReportPerVmNode.at( "/data" ) )
				.as( "health report per vm does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_vm_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "totalMemFree" ) ;
		String vmReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/vm" ) ;
		JsonNode vmReportNode = getJacksonMapper( ).readTree( vmReport ) ;
		logger.debug( "$$$$$$$ {} ", vmReport ) ;
		String expectedReport = "[{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"totalMemFree\":[11546338,76775728],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( vmReportNode.at( "/data" ) )
				.as( "vm report per vm does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_vm_report_divide_by_num_samples ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "totalMemFree" ) ;
		urlParams.put( "divideBy", "numberOfSamples" ) ;
		String vmReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/vm" ) ;
		JsonNode vmReportNode = getJacksonMapper( ).readTree( vmReport ) ;
		logger.debug( "$$$$$$$ {} ", vmReport ) ;
		String expectedReport = "[{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"totalMemFree\":[4373.612878787879,25065.533137446946],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( vmReportNode.at( "/data" ) )
				.as( "vm report divided by num samples does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_vm_report_divide_by_number ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ; // dates
																						// always
																						// roll
																						// back
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "totalMemFree" ) ;
		urlParams.put( "divideBy", "10" ) ;
		String vmReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/vm" ) ;

		JsonNode vmReportNode = getJacksonMapper( ).readTree( vmReport ) ;
		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( vmReportNode ) ) ;

		String expectedReport = "[{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"totalMemFree\":[1154633.8,7677572.8],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( vmReportNode.at( "/data" ) )
				.as( "vm report divided by number does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_service_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "topCpu" ) ;

		String serviceReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/service" ) ;
		JsonNode serviceReportNode = getJacksonMapper( ).readTree( serviceReport ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( serviceReportNode ) ) ;

		String expectedReport = "[{\"date\":[\"2019-01-31\",\"2019-02-01\"],\"topCpu\":[19324,19324],\"appId\":\"SensusCsap\",\"lifecycle\":\"dev\",\"project\":\"CSAP Platform\"}]" ;
		JsonNode expectedReportNode = getJacksonMapper( ).readTree( expectedReport ) ;

		assertThat( serviceReportNode.at( "/data" ) )
				.as( "service report does not match" )
				.isEqualTo( expectedReportNode ) ;

	}

	@Test
	public void verify_admin_service_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "topCpu" ) ;
		urlParams.put( "serviceName", "docker" ) ;

		String serviceReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/service" ) ;
		JsonNode adminTrendingReport = getJacksonMapper( ).readTree( serviceReport ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( adminTrendingReport ) ) ;

		List<String> dateList = jacksonMapper.readValue(
				adminTrendingReport.at( "/data/0/date" ).traverse( ), new TypeReference<List<String>>( ) {
				} ) ;

		assertThat( dateList )
				.as( "2 days of dates" )
				.hasSize( 2 )
				.contains( "2019-01-31", "2019-02-01" ) ;

		List<Integer> topList = jacksonMapper.readValue(
				adminTrendingReport.at( "/data/0/topCpu" ).traverse( ), new TypeReference<List<Integer>>( ) {
				} ) ;

		assertThat( topList )
				.as( "2 days of topCPU" )
				.hasSize( 2 )
				.contains( 620, 620 ) ;

	}

	@Test
	public void verify_merged_service_report_trends ( @Autowired MockMvc mockMvc )
		throws Exception {

		String servicesToMerge = "docker" + TrendingReportHelper.MULTIPLE_SERVICE_DELIMETER + "admin" ;
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "topCpu" ) ;
		urlParams.put( "serviceName", servicesToMerge ) ;

		String serviceReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/service" ) ;
		JsonNode adminTrendingReport = getJacksonMapper( ).readTree( serviceReport ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( adminTrendingReport ) ) ;

		List<String> dateList = jacksonMapper.readValue(
				adminTrendingReport.at( "/data/0/date" ).traverse( ), new TypeReference<List<String>>( ) {
				} ) ;

		assertThat( dateList )
				.as( "2 days of dates" )
				.hasSize( 2 )
				.contains( "2019-01-31", "2019-02-01" ) ;

		List<Integer> topList = jacksonMapper.readValue(
				adminTrendingReport.at( "/data/0/topCpu" ).traverse( ), new TypeReference<List<Integer>>( ) {
				} ) ;

		assertThat( topList )
				.as( "2 days of topCPU" )
				.hasSize( 2 )
				.contains( 620, 620 ) ;

	}

	@Test
	public void verify_merged_threads_report_trends_total ( @Autowired MockMvc mockMvc )
		throws Exception {

		String servicesToMerge = "docker" + TrendingReportHelper.MULTIPLE_SERVICE_DELIMETER + "admin" ;
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "2" ) ;
		urlParams.put( "trending", "true" ) ;
		urlParams.put( "metricsId", "threadCount" ) ;
		urlParams.put( "divideBy", "numberOfSamples" ) ;
		urlParams.put( "allVmTotal", "true" ) ;
		urlParams.put( "serviceName", servicesToMerge ) ;

		String serviceReport = retrieveAnalyticsReport( mockMvc, urlParams, "/api/report/service" ) ;
		JsonNode adminTrendingReport = getJacksonMapper( ).readTree( serviceReport ) ;

		logger.info( "Report Received: \n{} ",
				jacksonMapper.writerWithDefaultPrettyPrinter( ).writeValueAsString( adminTrendingReport ) ) ;

		assertThat( adminTrendingReport.get( "data" ).size( ) )
				.as( "when vmTotal is requested, separate feeds will be generated" )
				.isEqualTo( 1 ) ;

		List<String> dateList = jacksonMapper.readValue(
				adminTrendingReport.at( "/data/0/date" ).traverse( ), new TypeReference<List<String>>( ) {
				} ) ;

		assertThat( dateList )
				.as( "2 days of dates" )
				.hasSize( 2 )
				.contains( "2019-01-31", "2019-02-01" ) ;

		List<Double> topList = jacksonMapper.readValue(
				adminTrendingReport.at( "/data/0/threadCount" ).traverse( ), new TypeReference<List<Double>>( ) {
				} ) ;

		assertThat( topList )
				.as( "2 days of threadCount" )
				.hasSize( 2 )
				.contains( 38.43715846994535, 38.43715846994535 ) ;

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
