package org.csap.events.http.ui.rest ;

import java.util.Date ;

import javax.inject.Inject ;

import org.csap.docs.CsapDoc ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.admin.AdminDBHelper ;
import org.csap.events.db.EventDataWriter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.web.bind.annotation.GetMapping ;
import org.springframework.web.bind.annotation.RequestMapping ;
import org.springframework.web.bind.annotation.RestController ;

import com.fasterxml.jackson.core.JsonProcessingException ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ArrayNode ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.BasicDBList ;
import com.mongodb.BasicDBObject ;
import com.mongodb.CommandResult ;

@RestController
@RequestMapping ( CsapEventsApplication.HEALTH_API )
@CsapDoc ( title = "Data Health API" , type = CsapDoc.PUBLIC , notes = {
		"Exposes API for checking the health of system",
		"<a class='csap-link' target='_blank' href='https://github.com/csap-platform/csap-event-services'>learn more</a>",
		"<img class='csapDocImage' src='CSAP_BASE/images/event.png' />"
} )
public class ApiHealthController {
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;
	private String primaryHost ;
	@Inject
	private AdminDBHelper adminDBHelper ;
	@Inject
	private EventDataWriter eventDataWriter ;

	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	@CsapDoc ( notes = "Health of system. Checks mongo as well. " )
	@GetMapping ( )
	public String getHealthOfSystem ( ) {

		String health = "Pass" ;
		CommandResult result = adminDBHelper.getReplicaSetStatus( ) ;
		// logger.info("Result:: {}",result);
		BasicDBList dbList = (BasicDBList) result.get( "members" ) ;
		ObjectNode rootNode = null ;
		ArrayNode arrayNode = null ;

		if ( null != dbList ) {

			for ( Object obj : dbList ) {

				BasicDBObject dbObject = (BasicDBObject) obj ;

				if ( "PRIMARY".equalsIgnoreCase( dbObject.getString( "stateStr" ) ) ) {

					String host = dbObject.getString( "name" ) ;

					// logger.info("host {}",host);
					if ( null == primaryHost ) {

						primaryHost = host ;

					} else if ( ! primaryHost.equalsIgnoreCase( host ) ) {

						// post the event
						logger.info( "Primary switched" ) ;
						eventDataWriter.addPrimarySwitchedMessage( host, primaryHost ) ;
						primaryHost = host ;

					}

				}

				if ( dbObject.getInt( "health" ) == 0 ) {

					if ( null == rootNode || null == arrayNode ) {

						rootNode = jacksonMapper.createObjectNode( ) ;
						arrayNode = jacksonMapper.createArrayNode( ) ;

					}

					ObjectNode hostNode = jacksonMapper.createObjectNode( ) ;
					hostNode.put( "host", dbObject.getString( "name" ) ) ;
					hostNode.put( "stateStr", "not reachable" ) ;
					Date lastHeartBeat = dbObject.getDate( "lastHeartbeatRecv", null ) ;

					if ( null != lastHeartBeat ) {

						hostNode.put( "lastHeartBeat", lastHeartBeat.toString( ) ) ;

					}

					arrayNode.add( hostNode ) ;

				}

			}

		} else {

			logger.debug( "Command Result::{}", result ) ;
			health = "Fail" ;

		}

		if ( null != arrayNode ) {

			rootNode.set( "error", arrayNode ) ;

			try {

				String errorMessage = jacksonMapper.writeValueAsString( rootNode ) ;
				health = "Fail" + errorMessage ;

			} catch ( JsonProcessingException e ) {

				logger.error( "Error while converting to json", e ) ;

			}

		}

		return health ;

	}

}
