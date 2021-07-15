package org.csap.test.reports ;

import static org.assertj.core.api.Assertions.assertThat ;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content ;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status ;

import java.io.IOException ;
import java.net.URI ;
import java.net.URISyntaxException ;
import java.nio.file.Files ;
import java.nio.file.Paths ;
import java.text.DateFormat ;
import java.text.ParseException ;
import java.text.SimpleDateFormat ;
import java.util.HashSet ;
import java.util.Set ;

import javax.inject.Inject ;

import org.apache.commons.lang3.StringUtils ;
import org.csap.events.CsapEventsApplication ;
import org.csap.helpers.CsapApplication ;
import org.csap.test.container.CsapEventsTests ;
import org.junit.jupiter.api.AfterAll ;
import org.junit.jupiter.api.Tag ;
import org.junit.jupiter.api.Test ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.http.MediaType ;
import org.springframework.test.web.servlet.MockMvc ;
import org.springframework.test.web.servlet.ResultActions ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;

@CsapEventsTests.MockTests
@Tag ( "mongo" )

class EventApiTest {

	static {

		CsapApplication.initialize( "" ) ;

	}

	Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	String userId = "csapeng.gen" ;

	@Inject
	ObjectMapper jacksonMapper ;

	Set<String> insertedObjIds = new HashSet<>( ) ;

	@Inject
	MongoEmbedded mongoEmbedded ;

	@AfterAll
	void tearDown ( @Autowired MockMvc mockMvc )
		throws Exception {

		insertedObjIds.forEach( objId -> deleteEventById( mockMvc, objId ) ) ;

	}

	@Test
	void verify_delete_by_query ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		mongoEmbedded.reloadDefaultEventData( ) ;

		String searchQuery = "lifecycle=dev,appId=csapeng.gen,metaData.uiUser=paranant,eventReceivedOn=false,isDataRequired=false" ;
		String fileName = "ui_event.json" ;
		insertEvent( mockMvc, fileName ) ;
		insertEvent( mockMvc, fileName ) ;
		insertEvent( mockMvc, fileName ) ;
		fileName = "global_report.json" ;
		insertEvent( mockMvc, fileName ) ;

		try {

			URI uri = getUri( CsapEventsApplication.EVENT_API + "/deleteBySearch" ) ;

			ResultActions resultActions = mockMvc.perform( post( uri )
					.param( "searchString", searchQuery ).param( "userid", "csapeng.gen" ).param( "pass",
							"csaprest123!" )
					.contentType( MediaType.APPLICATION_FORM_URLENCODED ).accept( MediaType.APPLICATION_JSON ) ) ;
			String responseText = resultActions.andExpect( status( ).isOk( ) )
					.andExpect( content( ).contentType( MediaType.APPLICATION_JSON ) ).andReturn( ).getResponse( )
					.getContentAsString( ) ;
			JsonNode jsonResults = jacksonMapper.readTree( responseText ) ;

			assertThat( jsonResults.at( "/result" ).asText( ) )
					.isEqualTo( "Deleted 3 record " ) ;
			// assertEquals( "Could not delete the record", "Deleted 3 record ",
			// jsonResults.at( "/result" ).asText() ) ;

		} catch ( Exception e ) {

			logger.error( "Exception ", e ) ;

		}

	}

	@Test
	public void verify_Metrics ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String fileName = "metrics.json" ;
		insertEvent( mockMvc, fileName ) ;

	}

	@Test
	public void verify_global_host_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		mongoEmbedded.reloadDefaultEventData( ) ;

		String fileName = "global_report.json" ;
		logger.info( "Starting test" ) ;
		insertEvent( mockMvc, fileName ) ;
		String fileContent = readFile( fileName ) ;
		JsonNode insertedEvent = jacksonMapper.readTree( fileContent ) ;
		JsonNode retrievedEvent = getLatestEvent( mockMvc, insertedEvent.at( "/category" ).asText( ), userId,
				insertedEvent.at( "/lifecycle" ).asText( ) ) ;

		assertThat( insertedEvent.at( "/category" ).asText( ) )
				.as( "Category does not match" )
				.isEqualTo( retrievedEvent.at( "/category" ).asText( ) ) ;
		assertThat( retrievedEvent.has( "expiresAt" ) )
				.as( "Expires does not exists in created event" )
				.isTrue( ) ;
		assertThat( convertDate( retrievedEvent.at( ( "/expiresAt" ) ).asText( ) ) )
				.as( "Expires time is not greater than current time" )
				.isGreaterThan( System.currentTimeMillis( ) ) ;

		assertThat( retrievedEvent.at( "/createdOn/mongoDate" ).isMissingNode( ) )
				.as( "Created event does not have mongo date" )
				.isFalse( ) ;
		assertThat( retrievedEvent.at( "/createdOn/lastUpdatedOn" ).isMissingNode( ) )
				.as( "Created event does not have last updated on" )
				.isFalse( ) ;
		assertThat( retrievedEvent.at( "/counter" ).asInt( ) )
				.as( "Counter does not match" )
				.isEqualTo( 1 ) ;

		fileName = "global_report_diff_host.json" ;
		insertEvent( mockMvc, fileName ) ;
		fileContent = readFile( fileName ) ;
		JsonNode updatedEventFromFile = jacksonMapper.readTree( fileContent ) ;
		JsonNode updatedEventFromDb = getLatestEvent( mockMvc, updatedEventFromFile.at( "/category" ).asText( ), userId,
				updatedEventFromFile.at( "/lifecycle" ).asText( ) ) ;
		JsonNode countEvents = countEvents( mockMvc, insertedEvent.at( "/category" ).asText( ), userId,
				insertedEvent.at( "/lifecycle" ).asText( ) ) ;

		assertThat( countEvents.at( "/count" ).asInt( ) )
				.as( "Number of events does not match" )
				.isEqualTo( 1 ) ;

		assertThat( updatedEventFromFile.at( "/host" ).asText( ) )
				.as( "Host does not match" )
				.isEqualTo( updatedEventFromDb.at( "/host" ).asText( ) ) ;
		assertThat( updatedEventFromDb.at( "/counter" ).asInt( ) )
				.as( "Counter does not match" )
				.isEqualTo( 2 ) ;
		assertThat( updatedEventFromFile.at( "/data" ) )
				.as( "Data does not match" )
				.isEqualTo( updatedEventFromDb.at( "/data" ) ) ;

	}

	@Test
	public void verify_host_report ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String fileName = "host_report.json" ;
		insertEvent( mockMvc, fileName ) ;
		String fileContent = readFile( fileName ) ;
		JsonNode insertedEvent = jacksonMapper.readTree( fileContent ) ;
		JsonNode retrievedEvent = getLatestEvent( mockMvc, insertedEvent.at( "/category" ).asText( ), userId,
				insertedEvent.at( "/lifecycle" ).asText( ) ) ;

		assertThat( insertedEvent.at( "/category" ).asText( ) )
				.as( "Category does not match" )
				.isEqualTo( retrievedEvent.at( "/category" ).asText( ) ) ;
		assertThat( retrievedEvent.has( "expiresAt" ) )
				.as( "Expires does not exists in created event" )
				.isTrue( ) ;
		assertThat( convertDate( retrievedEvent.at( ( "/expiresAt" ) ).asText( ) ) )
				.as( "Expires time is not greater than current time" )
				.isGreaterThan( System.currentTimeMillis( ) ) ;

		assertThat( retrievedEvent.at( "/createdOn/mongoDate" ).isMissingNode( ) )
				.as( "Created event does not have mongo date" )
				.isFalse( ) ;
		assertThat( retrievedEvent.at( "/createdOn/lastUpdatedOn" ).isMissingNode( ) )
				.as( "Created event does not have last updated on" )
				.isFalse( ) ;
		assertThat( retrievedEvent.at( "/counter" ).asInt( ) )
				.as( "Counter does not match" )
				.isEqualTo( 1 ) ;

		fileName = "updated_host_report.json" ;
		insertEvent( mockMvc, fileName ) ;
		fileContent = readFile( fileName ) ;
		JsonNode updatedEventFromFile = jacksonMapper.readTree( fileContent ) ;
		JsonNode updatedEventFromDb = getLatestEvent( mockMvc, updatedEventFromFile.at( "/category" ).asText( ), userId,
				updatedEventFromFile.at( "/lifecycle" ).asText( ) ) ;

		assertThat( updatedEventFromDb.at( "/counter" ).asInt( ) )
				.as( "Counter does not match" )
				.isEqualTo( 2 ) ;
		assertThat( updatedEventFromFile.at( "/data" ) )
				.as( "Data does not match" )
				.isEqualTo( updatedEventFromDb.at( "/data" ) ) ;

	}

	@Test
	public void verify_memory_low_event ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String fileName = "memory_low.json" ;
		insertEvent( mockMvc, fileName ) ;
		String fileContent = readFile( fileName ) ;
		JsonNode insertedEvent = jacksonMapper.readTree( fileContent ) ;
		JsonNode retrievedEvent = getLatestEvent( mockMvc, insertedEvent.at( "/category" ).asText( ), userId,
				insertedEvent.at( "/lifecycle" ).asText( ) ) ;

		assertThat( insertedEvent.at( "/category" ).asText( ) )
				.as( "Category does not match" )
				.isEqualTo( retrievedEvent.at( "/category" ).asText( ) ) ;
		assertThat( retrievedEvent.has( "expiresAt" ) )
				.as( "Expires does not exists in created event" )
				.isTrue( ) ;
		assertThat( convertDate( retrievedEvent.at( ( "/expiresAt" ) ).asText( ) ) )
				.as( "Expires time is not greater than current time" )
				.isGreaterThan( System.currentTimeMillis( ) ) ;

		assertThat( retrievedEvent.at( "/createdOn/mongoDate" ).isMissingNode( ) )
				.as( "Created event does not have mongo date" )
				.isFalse( ) ;
		assertThat( retrievedEvent.at( "/createdOn/lastUpdatedOn" ).isMissingNode( ) )
				.as( "Created event does not have last updated on" )
				.isFalse( ) ;
		assertThat( retrievedEvent.at( "/counter" ).asInt( ) )
				.as( "Counter does not match" )
				.isEqualTo( 1 ) ;

		fileName = "updated_memory_low.json" ;
		insertEvent( mockMvc, fileName ) ;
		fileContent = readFile( fileName ) ;
		JsonNode updatedEventFromFile = jacksonMapper.readTree( fileContent ) ;
		JsonNode updatedEventFromDb = getLatestEvent( mockMvc, updatedEventFromFile.at( "/category" ).asText( ), userId,
				updatedEventFromFile.at( "/lifecycle" ).asText( ) ) ;

		assertThat( updatedEventFromDb.at( "/counter" ).asInt( ) )
				.as( "Counter does not match" )
				.isEqualTo( 2 ) ;
		assertThat( updatedEventFromFile.at( "/data" ) )
				.as( "Data does not match" )
				.isEqualTo( updatedEventFromDb.at( "/data" ) ) ;

	}

	@Test
	public void verify_ui_event ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String fileName = "ui_event.json" ;
		insertEvent( mockMvc, fileName ) ;
		String fileContent = readFile( fileName ) ;
		JsonNode insertedEvent = jacksonMapper.readTree( fileContent ) ;
		JsonNode retrievedEvent = getLatestEvent( mockMvc, insertedEvent.at( "/category" ).asText( ), userId,
				insertedEvent.at( "/lifecycle" ).asText( ) ) ;

		assertThat( insertedEvent.at( "/category" ).asText( ) )
				.as( "Category does not match" )
				.isEqualTo( retrievedEvent.at( "/category" ).asText( ) ) ;
		assertThat( retrievedEvent.has( "expiresAt" ) )
				.as( "Expires does not exists in created event" )
				.isTrue( ) ;

		logger.info( "retrieved expiresAt: {}", retrievedEvent.at( ( "/expiresAt" ) ).asText( ) ) ;

		assertThat( convertDate( retrievedEvent.at( ( "/expiresAt" ) ).asText( ) ) )
				.as( "Expires time is not greater than current time" )
				.isGreaterThan( System.currentTimeMillis( ) ) ;

		assertThat( insertedEvent.at( "/metaData" ) )
				.as( "Data does not match" )
				.isEqualTo( retrievedEvent.at( "/metaData" ) ) ;
		assertThat( retrievedEvent.at( "/createdOn/mongoDate" ).isMissingNode( ) )
				.as( "Created event does not have mongo date" )
				.isFalse( ) ;
		assertThat( retrievedEvent.at( "/createdOn/lastUpdatedOn" ).isMissingNode( ) )
				.as( "Created event does not have last updated on" )
				.isFalse( ) ;

	}

	@Test
	public void verify_settings ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String fileName = "event_settings.json" ;
		insertEvent( mockMvc, fileName ) ;
		String fileContent = readFile( fileName ) ;
		JsonNode insertedEvent = jacksonMapper.readTree( fileContent ) ;
		JsonNode retrievedEvent = getLatestEvent( mockMvc, insertedEvent.at( "/category" ).asText( ), userId,
				insertedEvent.at( "/lifecycle" ).asText( ) ) ;
		assertThat( insertedEvent.at( "/category" ).asText( ) )
				.as( "Category does not match" )
				.isEqualTo( retrievedEvent.at( "/category" ).asText( ) ) ;
		assertThat( retrievedEvent.has( "expiresAt" ) )
				.as( "Expires exists in created event" )
				.isFalse( ) ;

		assertThat( retrievedEvent.at( "/createdOn/mongoDate" ).isMissingNode( ) )
				.as( "Created event does not have mongo date" )
				.isFalse( ) ;
		assertThat( retrievedEvent.at( "/createdOn/lastUpdatedOn" ).isMissingNode( ) )
				.as( "Created event does not have last updated on" )
				.isFalse( ) ;

	}

	@Test
	public void verify_insert_event ( @Autowired MockMvc mockMvc )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		String fileName = "analytics_settings.json" ;
		String fileContent = readFile( fileName ) ;
		URI uri = getUri( CsapEventsApplication.EVENT_API + "/insert" ) ;

		ResultActions resultActions = mockMvc.perform( post( uri ).param( "eventJson", fileContent )
				.param( "userid", "csapeng.gen" ).param( "pass", "csaprest123!" )
				.param( "appId", "csapeng.gen" ).param( "life", "dev" ).param( "project", "Test Project" ).param(
						"summary", "Test insert" )
				.param( "category", "/csap/settings/uisettings" )
				.contentType( MediaType.APPLICATION_FORM_URLENCODED ).accept( MediaType.APPLICATION_JSON ) ) ;
		String responseText = resultActions.andExpect( status( ).isOk( ) )
				// .andExpect(content().contentType(MediaType.APPLICATION_JSON))
				.andReturn( )
				.getResponse( )
				.getContentAsString( ) ;

		String searchQuery = "lifecycle=dev,appId=csapeng.gen,simpleSearchText=/csap/settings/uisettings,eventReceivedOn=false,isDataRequired=false" ;

		try {

			uri = getUri( CsapEventsApplication.EVENT_API + "/deleteBySearch" ) ;

			resultActions = mockMvc.perform( post( uri )
					.param( "searchString", searchQuery ).param( "userid", "csapeng.gen" ).param( "pass",
							"csaprest123!" )
					.contentType( MediaType.APPLICATION_FORM_URLENCODED ).accept( MediaType.APPLICATION_JSON ) ) ;
			responseText = resultActions.andExpect( status( ).isOk( ) )
					.andExpect( content( ).contentType( MediaType.APPLICATION_JSON ) ).andReturn( ).getResponse( )
					.getContentAsString( ) ;
			JsonNode jsonResults = jacksonMapper.readTree( responseText ) ;
			// assertEquals( "Could not delete the record", "Deleted 1 record ",
			// jsonResults.at( "/result" ).asText() ) ;

			assertThat( jsonResults.at( "/result" ).asText( ) )
					.isEqualTo( "Deleted 1 record " ) ;

		} catch ( Exception e ) {

			logger.error( "Exception ", e ) ;

		}

	}

	private JsonNode getLatestEvent ( MockMvc mockMvc , String category , String appId , String life )
		throws Exception {

		URI uri = getUri( CsapEventsApplication.EVENT_API + "/latest" ) ;

		ResultActions resultActions = mockMvc.perform( post( uri ).param( "category", category )
				.param( "appId", appId ).param( "life", life ).accept( MediaType.APPLICATION_JSON ) ) ;

		String responseText = resultActions.andExpect( status( ).isOk( ) )
				.andExpect( content( ).contentType( MediaType.APPLICATION_JSON ) ).andReturn( ).getResponse( )
				.getContentAsString( ) ;

		JsonNode latestEvent = jacksonMapper.readTree( responseText ) ;
		return latestEvent ;

	}

	private JsonNode countEvents ( MockMvc mockMvc , String category , String appId , String life )
		throws Exception {

		URI uri = getUri( CsapEventsApplication.EVENT_API + "/count" ) ;

		ResultActions resultActions = mockMvc.perform( post( uri ).param( "category", category )
				.param( "appId", appId ).param( "life", life ).param( "days", "-1" ).accept(
						MediaType.APPLICATION_JSON ) ) ;

		String responseText = resultActions.andExpect( status( ).isOk( ) )
				.andExpect( content( ).contentType( MediaType.APPLICATION_JSON ) ).andReturn( ).getResponse( )
				.getContentAsString( ) ;

		JsonNode latestEvent = jacksonMapper.readTree( responseText ) ;
		return latestEvent ;

	}

	private String insertEvent ( MockMvc mockMvc , String fileName )
		throws Exception {

		String fileContent = readFile( fileName ) ;
		URI uri = getUri( CsapEventsApplication.EVENT_API ) ;

		ResultActions resultActions = mockMvc.perform( post( uri ).param( "eventJson", fileContent )
				.param( "userid", "csapeng.gen" ).param( "pass", "csaprest123!" )
				.contentType( MediaType.APPLICATION_FORM_URLENCODED ).accept( MediaType.APPLICATION_JSON ) ) ;

		String responseText = resultActions.andExpect( status( ).isOk( ) )
				// .andExpect(content().contentType(MediaType.APPLICATION_JSON))
				.andReturn( )
				.getResponse( )
				.getContentAsString( ) ;

		// result is in the form "Added Event: XXXXXX"
		logger.info( "responseText: {}", responseText ) ;
		String result = responseText.substring( "Added Event: ".length( ) ) ;

		logger.info( "Object id{}", result ) ;

		if ( StringUtils.isBlank( result ) || "Success".equalsIgnoreCase( result )
				|| "Failure".equalsIgnoreCase( result ) ) {

			logger.error( "Did not find object id. Deletion will not happen" ) ;

		} else {

			insertedObjIds.add( result ) ;

		}

		return responseText ;

	}

	private void deleteEventById ( MockMvc mockMvc , String objectId ) {

		try {

			URI uri = getUri( CsapEventsApplication.EVENT_API + "/delete" ) ;
			ResultActions resultActions = mockMvc.perform( post( uri )
					.param( "objectId", objectId ).param( "userid", "csapeng.gen" ).param( "pass", "csaprest123!" )
					.contentType( MediaType.APPLICATION_FORM_URLENCODED ).accept( MediaType.APPLICATION_JSON ) ) ;
			String responseText = resultActions.andExpect( status( ).isOk( ) )
					.andExpect( content( ).contentType( MediaType.APPLICATION_JSON ) ).andReturn( ).getResponse( )
					.getContentAsString( ) ;
			JsonNode jsonResults = jacksonMapper.readTree( responseText ) ;
			logger.info( "Delete result for object id {} is {} ", objectId, jsonResults.at( "/result" ).asText( ) ) ;
			// assertEquals("Could not delete the record", "Deleted 1 record ",
			// jsonResults.at("/result").asText());

		} catch ( Exception e ) {

			logger.error( "Exception ", e ) ;

		}

	}

	private URI getUri ( String subUri )
		throws URISyntaxException {

		return new URI( subUri ) ;

	}

	private String readFile ( String fileName )
		throws IOException ,
		URISyntaxException {

		ClassLoader classloader = getClass( ).getClassLoader( ) ;
		return new String( Files.readAllBytes( Paths.get( classloader.getResource( fileName ).toURI( ) ) ) ) ;

	}

	private long convertDate ( String date )
		throws ParseException {

		// 2021-08-06T16:45:54.128+00:00
//		DateFormat df = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSZ" ) ;
		DateFormat df = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSX" ) ;
		return df.parse( date ).getTime( ) ;

	}
}
