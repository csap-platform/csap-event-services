package org.csap.events.http.ui.rest ;

import javax.inject.Inject ;
import javax.servlet.http.HttpSession ;

import org.apache.commons.logging.Log ;
import org.apache.commons.logging.LogFactory ;
import org.csap.docs.CsapDoc ;
import org.csap.events.CsapEventsApplication ;
import org.csap.security.config.CsapSecurityRoles ;
import org.csap.security.config.CsapSecuritySettings ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.http.MediaType ;
import org.springframework.web.bind.annotation.RequestMapping ;
import org.springframework.web.bind.annotation.RequestMethod ;
import org.springframework.web.bind.annotation.RestController ;
import org.springframework.web.servlet.ModelAndView ;

import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ObjectNode ;
import com.mongodb.MongoClient ;

@RestController
@RequestMapping ( CsapEventsApplication.API_URL )
@CsapDoc ( title = "CSAP reports API" , type = CsapDoc.PUBLIC , notes = {
		"CSAP analytics reports api",
		"<a class='csap-link' target='_blank' href='https://github.com/csap-platform/csap-core/wiki'>learn more</a>"
} )
public class ApiController {
	protected final Log logger = LogFactory.getLog( getClass( ) ) ;

	@Inject
	private MongoClient mongoClient ;

	@Autowired ( required = false )
	private CsapSecuritySettings securitySettings ;

	@RequestMapping ( method = RequestMethod.GET )
	public ModelAndView get ( ) {

		logger.info( "Got help" ) ;
		return new ModelAndView( "redirect:" + CsapEventsApplication.API_URL + "/help" ) ;

	}

	@RequestMapping ( "/help" )
	public ModelAndView help ( ) {

		logger.info( "Got help" ) ;
		return new ModelAndView( CsapEventsApplication.API_URL + "/help" ) ;

	}

	@RequestMapping ( "/trendReport" )
	public ModelAndView trendReport ( ) {

		return new ModelAndView( CsapEventsApplication.API_URL + "/trend" ) ;

	}

	@RequestMapping ( "/hosts" )
	public ModelAndView hosts ( ) {

		return new ModelAndView( CsapEventsApplication.API_URL + "/hosts" ) ;

	}

	@RequestMapping ( value = "/isAuthorized" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode isAuthorized ( HttpSession session ) {

		ObjectNode result = jacksonMapper.createObjectNode( ) ;

		boolean isAdmin = true ;

		if ( securitySettings != null ) {

			if ( ! securitySettings.getRoles( ).getAndStoreUserRoles( session )
					.contains( CsapSecurityRoles.ADMIN_ROLE ) ) {

				isAdmin = false ;

			}

		}

		result.put( "isAuthorized", isAdmin ) ;

		// logger.info("isAuthorized {}",isAuthorized);
		return result ;

	}

	private ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	@RequestMapping ( value = "/helloJson" , produces = MediaType.APPLICATION_JSON_VALUE )
	public ObjectNode helloJson ( ) {

		ObjectNode resultNode = jacksonMapper.createObjectNode( ) ;
		resultNode.put( "message", "Hello" ) ;
		return resultNode ;

	}

	// @RequestMapping("/health")
	// public String getHealthOfSystem(){
	// String health = "Pass";
	// CommandResult result =
	// mongoClient.getDB("admin").command("replSetGetStatus");
	// BasicDBList dbList = (BasicDBList)result.get("members");
	// ObjectNode rootNode = null;
	// ArrayNode arrayNode = null;
	// if(null != dbList){
	// for(Object obj : dbList){
	// BasicDBObject dbObject = (BasicDBObject) obj;
	// if(dbObject.getInt("health") == 0){
	// if(null == rootNode || null == arrayNode){
	// rootNode = jacksonMapper.createObjectNode();
	// arrayNode = jacksonMapper.createArrayNode();
	// }
	//
	// ObjectNode hostNode = jacksonMapper.createObjectNode();
	// hostNode.put("host",dbObject.getString("name"));
	// hostNode.put("stateStr","not reachable");
	// Date lastHeartBeat = dbObject.getDate("lastHeartbeatRecv", null);
	// if(null != lastHeartBeat){
	// hostNode.put("lastHeartBeat",lastHeartBeat.toString());
	// }
	// arrayNode.add(hostNode);
	// }
	// }
	// } else {
	// health="Fail";
	// }
	// if(null != arrayNode){
	// rootNode.set("error",arrayNode);
	// try {
	// String errorMessage = jacksonMapper.writeValueAsString(rootNode);
	// health = "Fail" + errorMessage;
	// } catch (JsonProcessingException e) {
	// logger.error("Error while converting to json",e);
	// }
	// }
	// //call metrics
	// //systemHealthMonitor.monitorMetrics();
	// return health;
	// }

}
