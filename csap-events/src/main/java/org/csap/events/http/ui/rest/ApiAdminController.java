package org.csap.events.http.ui.rest ;

import java.util.ArrayList ;
import java.util.HashMap ;
import java.util.List ;
import java.util.Map ;
import java.util.Set ;

import javax.inject.Inject ;
import javax.servlet.http.HttpSession ;

import org.bson.Document ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.admin.AdminDBHelper ;
import org.csap.events.admin.ClientOptions ;
import org.csap.events.admin.Stats ;
import org.csap.security.config.CsapSecurityRoles ;
import org.csap.security.config.CsapSecuritySettings ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.http.MediaType ;
import org.springframework.web.bind.annotation.RequestMapping ;
import org.springframework.web.bind.annotation.RequestParam ;
import org.springframework.web.bind.annotation.RestController ;

import com.fasterxml.jackson.core.JsonProcessingException ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.SerializationFeature ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.MongoClientOptions ;
import com.mongodb.client.MongoIterable ;

@RestController
@RequestMapping ( CsapEventsApplication.ADMIN_API )
public class ApiAdminController {

	final Logger logger = LoggerFactory.getLogger( getClass( ) ) ;
	@Inject
	private AdminDBHelper adminDBHelper ;
	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	@Autowired ( required = false )
	private CsapSecuritySettings securitySettings ;

	@RequestMapping ( value = "/dbs" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String getDbNames ( ) {

		String result = "" ;
		MongoIterable<String> dbs = adminDBHelper.getDBNames( ) ;

		try {

			result = jacksonMapper.writeValueAsString( dbs ) ;

		} catch ( JsonProcessingException e ) {

			logger.error( "Exception while converting db names to string", e ) ;

		}

		return result ;

	}

	@RequestMapping ( value = "/colls" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String getCollections (
									@RequestParam ( value = "dbname" , required = false , defaultValue = "cssp" ) String dbname ) {

		String result = "" ;
		MongoIterable<String> colls = adminDBHelper.getCollectionNames( dbname ) ;

		try {

			result = jacksonMapper.writeValueAsString( colls ) ;

		} catch ( JsonProcessingException e ) {

			logger.error( "Exception while converting colls names to string", e ) ;

		}

		return result ;

	}

	@RequestMapping ( value = "/dbStatus" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String getDbStatus (
								@RequestParam ( value = "dbname" , required = false , defaultValue = "cssp" ) String dbname ) {

		String result = "" ;
		Document primaryDoc = adminDBHelper.getDbStatsFromPrimary( dbname ) ;
		Document secondaryDoc = adminDBHelper.getDbStatsFromSecondary( dbname ) ;

		try {

			List<Stats> dbStatusList = mergeResult( primaryDoc, secondaryDoc ) ;
			result = jacksonMapper.writeValueAsString( dbStatusList ) ;
			logger.debug( "after dbstatus" ) ;

		} catch ( JsonProcessingException e ) {

			logger.error( "Exception while converting colls names to string", e ) ;

		}

		return result ;

	}

	@RequestMapping ( value = "/collStatus" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String getCollStatus (
									@RequestParam ( value = "dbname" , required = false , defaultValue = "cssp" ) String dbname ,
									@RequestParam ( value = "collName" , required = false , defaultValue = "analytics" ) String collName ) {

		String result = "" ;
		Document primaryDoc = adminDBHelper.getCollectionStatsFromPrimary( dbname, collName ) ;
		Document secondaryDoc = adminDBHelper.getCollectionStatsFromSecondary( dbname, collName ) ;

		try {

			List<Stats> collStatusList = mergeResult( primaryDoc, secondaryDoc ) ;
			result = jacksonMapper.writeValueAsString( collStatusList ) ;

		} catch ( JsonProcessingException e ) {

			logger.error( "Exception while converting colls names to string", e ) ;

		}

		return result ;

	}

	@RequestMapping ( value = "/mongoOptions" , produces = MediaType.APPLICATION_JSON_VALUE )
	public String getMongoOptions ( ) {

		String result = "" ;
		Map<String, Object> options = new HashMap<>( ) ;
		MongoClientOptions mongoOptions = adminDBHelper.getMongoOptions( ) ;
		ClientOptions clientOptions = new ClientOptions( mongoOptions ) ;
		options.put( "mongoOptions", clientOptions ) ;
		options.put( "driverInfo", adminDBHelper.getDriverVersion( ) ) ;

		try {

			jacksonMapper.configure( SerializationFeature.FAIL_ON_EMPTY_BEANS, false ) ;
			// result = JSON.serialize(mongoOptions);
			result = jacksonMapper.writeValueAsString( options ) ;
			logger.debug( "result {} " + result ) ;

		} catch ( Exception e ) {

			logger.error( "Error while converting mongo options", e ) ;

		}

		return result ;

	}

	@RequestMapping ( value = "/replicaStepDown" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode stepDown ( HttpSession session ) {

		boolean isAdmin = true ;
		ObjectNode result = jacksonMapper.createObjectNode( ) ;

		if ( securitySettings != null ) {

			if ( ! securitySettings.getRoles( ).getAndStoreUserRoles( session )
					.contains( CsapSecurityRoles.ADMIN_ROLE ) ) {

				isAdmin = false ;

			}

		}

		if ( isAdmin ) {

			adminDBHelper.replicaSetStepDown( ) ;
			result.put( "success", "Replica step down" ) ;

		} else {

			result.put( "error", "access denied" ) ;

		}

		return result ;

	}

	private List<Stats> mergeResult ( Document primary , Document secondary ) {

		List<Stats> collStatusList = new ArrayList<>( ) ;
		Set<String> keySet = null ;

		if ( null != primary ) {

			keySet = primary.keySet( ) ;

		} else if ( null != secondary ) {

			keySet = secondary.keySet( ) ;

		}

		for ( String key : keySet ) {

			Stats collStatus = new Stats( ) ;
			collStatus.setKey( key ) ;

			if ( null != primary ) {

				collStatus.setPrimaryValue( "" + primary.get( key ) ) ;

			}

			if ( null != secondary ) {

				collStatus.setSecondaryValue( "" + secondary.get( key ) ) ;

			}

			collStatusList.add( collStatus ) ;

		}

		String primaryHost = adminDBHelper.getPrimary( ) ;
		String secondaryHost = adminDBHelper.getSecondary( ) ;
		Stats stats = new Stats( ) ;
		stats.setKey( "serverUsed" ) ;
		stats.setPrimaryValue( primaryHost ) ;
		stats.setSecondaryValue( secondaryHost ) ;
		collStatusList.add( stats ) ;
		return collStatusList ;

	}
}
