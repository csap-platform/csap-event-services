package org.csap.events ;

import java.io.PrintWriter ;
import java.security.Principal ;
import java.time.LocalDateTime ;
import java.time.format.DateTimeFormatter ;

import javax.inject.Inject ;
import javax.servlet.http.HttpSession ;

import org.apache.commons.lang3.text.WordUtils ;
import org.csap.CsapMonitor ;
import org.csap.docs.CsapDoc ;
import org.csap.events.db.AnalyticsHelper ;
import org.csap.events.db.HealthEventWriter ;
import org.csap.helpers.CsapApplication ;
import org.csap.integations.CsapInformation ;
import org.csap.security.CustomUserDetails ;
import org.csap.security.config.CsapSecurityRoles ;
import org.csap.security.config.CsapSecuritySettings ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.security.core.context.SecurityContextHolder ;
import org.springframework.stereotype.Controller ;
import org.springframework.ui.Model ;
import org.springframework.ui.ModelMap ;
import org.springframework.web.bind.annotation.GetMapping ;
import org.springframework.web.bind.annotation.RequestMapping ;
import org.springframework.web.bind.annotation.RequestMethod ;
import org.springframework.web.bind.annotation.RequestParam ;
import org.springframework.web.bind.annotation.ResponseBody ;
import org.springframework.web.servlet.ModelAndView ;

@Controller
@CsapMonitor
@RequestMapping ( "/" )
@CsapDoc ( title = "CSAP Landing Page Controller" , type = CsapDoc.PUBLIC , notes = {
		"Landing page provides simple technology demonstrations. Refer to @CsapDoc java doc for more usage examples.",
		"<a class='csap-link' target='_blank' href='https://github.com/csap-platform/csap-core/wiki'>learn more</a>"
} )
public class LandingPage {

	final Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Autowired
	CsapInformation csapInformation ;

	@Inject
	private AnalyticsHelper analyticsHelper ;

	@Autowired ( required = false )
	private CsapSecuritySettings securitySettings ;

	/*
	 * @RequestMapping("/test") public ModelAndView getTest() { ModelAndView
	 * modelAndView = new ModelAndView("test");
	 * modelAndView.addObject("projectNames", analyticsHelper.getProjectNames());
	 * return modelAndView; }
	 */

	@RequestMapping ( "/stats" )
	public String stats ( ) {

		logger.info( "inside stats page" ) ;
		return "dbstats/stats" ;

	}

	// @GetMapping ( value = { "/" } )
	// public String summary ( Model springViewModel ) {
	// logger.debug( "in here", csapInformation.getLifecycle() );
	// springViewModel.addAttribute( "csapPageLabel", "Csap Activity And Adoption
	// Portal : 7 Day Summary" );
	// // springViewModel.addAttribute("csapPageLabel", "7 Day summary");
	// return "projects/adoption";
	// }

	@GetMapping ( value = {
			"/analytics"
	} )
	public String summary ( Model springViewModel ) {

		logger.debug( "in here", csapInformation.getLifecycle( ) ) ;
		springViewModel.addAttribute( "csapPageLabel", "Csap Adoption Portal : 7 Day Summary" ) ;
		// springViewModel.addAttribute("csapPageLabel", "7 Day summary");
		return "projects/adoption" ;

	}

	@GetMapping ( "/trends" )
	public ModelAndView analytics ( Model springViewModel ) {

		logger.debug( "in details" ) ;

		springViewModel.addAttribute( "analyticsPortal", csapInformation.getLoadBalancerUrl( )
				+ "/csap-admin/os/performance" ) ;
		return new ModelAndView( "projects/trends" ) ;

	}

	@GetMapping ( "/health" )
	public ModelAndView health ( ) {

		return new ModelAndView( "projects/health" ) ;

	}

	@GetMapping ( "/services" )
	public ModelAndView instance ( ) {

		return new ModelAndView( "projects/services" ) ;

	}

	@GetMapping ( "/admin" )
	public ModelAndView admin ( ) {

		return new ModelAndView( "admin/admin" ) ;

	}

	@RequestMapping ( "/test" )
	public String get ( Model springViewModel ) {

		var hostname = csapInformation.getHostName( ) ;

		if ( ! CsapApplication.isCsapFolderSet( ) ) {

			hostname = "csap-dev01" ;

		}

		springViewModel.addAttribute( "hostname", hostname ) ;

		return "LandingPage" ;

	}

	@RequestMapping ( "/help" )
	public String help ( Model springViewModel ) {

		logger.debug( "Inside" ) ;
		springViewModel.addAttribute( "host", csapInformation.getHostName( ) ) ;
		springViewModel.addAttribute( "name", csapInformation.getName( ) ) ;
		springViewModel.addAttribute( "version", csapInformation.getVersion( ) ) ;

		springViewModel.addAttribute( "dateTime",
				LocalDateTime.now( ).format( DateTimeFormatter.ofPattern( "HH:mm:ss,   MMMM d  uuuu " ) ) ) ;

		return "help/help" ;

	}

	@RequestMapping ( "/currentTime" )
	public void currentTime ( PrintWriter writer ) {

		String formatedTime = LocalDateTime.now( ).format( DateTimeFormatter.ofPattern(
				"HH:mm:ss,   MMMM d  uuuu " ) ) ;

		logger.info( "Time now is: {}", formatedTime ) ;

		writer.println( "currentTime: " + formatedTime ) ;

		return ;

	}

	@RequestMapping ( "/currentUser" )
	public void currentUser ( PrintWriter writer , Principal principle ) {

		logger.info( "SpringMvc writer" ) ;

		writer.println( "logged in user: " + principle.getName( ) ) ;
		return ;

	}

	@RequestMapping ( "/currentUserDetails" )
	public void currentUserDetails ( PrintWriter writer ) {

		logger.info( "SpringMvc writer" ) ;
		CustomUserDetails userDetails = (CustomUserDetails) SecurityContextHolder.getContext( )
				.getAuthentication( )
				.getPrincipal( ) ;
		;
		writer.println( "logged in user email: " + userDetails.getMail( ) ) ;
		writer.println( "\n\n user information: \n" + WordUtils.wrap( userDetails.toString( ), 80 ) ) ;

		return ;

	}

	@RequestMapping ( "/testNullPointer" )
	public String testNullPointer ( ) {

		if ( System.currentTimeMillis( ) > 1 )
			throw new NullPointerException( "For testing only" ) ;

		return "hello" ;

	}

	@RequestMapping ( "/missingTemplate" )
	public String missingTempate ( Model springViewModel ) {

		logger.info( "Sample thymeleaf controller" ) ;

		springViewModel.addAttribute( "dateTime",
				LocalDateTime.now( ).format( DateTimeFormatter.ofPattern( "HH:mm:ss,   MMMM d  uuuu " ) ) ) ;

		// templates are in: resources/templates/*.html
		// leading "/" is critical when running in a jar

		return "/missingTemplate" ;

	}

	@RequestMapping ( "/malformedTemplate" )
	public String malformedTemplate ( Model springViewModel ) {

		logger.info( "Sample thymeleaf controller" ) ;

		springViewModel.addAttribute( "dateTime",
				LocalDateTime.now( ).format( DateTimeFormatter.ofPattern( "HH:mm:ss,   MMMM d  uuuu " ) ) ) ;

		// templates are in: resources/templates/*.html
		// leading "/" is critical when running in a jar

		return "/MalformedExample" ;

	}

	@RequestMapping ( method = RequestMethod.GET )
	public String get ( ModelMap springViewModel , HttpSession session ) {

		boolean isAdmin = true ;

		if ( securitySettings != null ) {

			if ( ! securitySettings.getRoles( ).getAndStoreUserRoles( session )
					.contains( CsapSecurityRoles.ADMIN_ROLE ) ) {

				isAdmin = false ;

			}

		}

		springViewModel.addAttribute( "admin", isAdmin ) ;
		addCommonAttributes( springViewModel ) ;
		return "events/events" ;

	}

	@RequestMapping ( "/events" )
	public String events ( ModelMap springViewModel ) {

		addCommonAttributes( springViewModel ) ;
		return "events/events" ;

	}

	@Autowired
	CsapEventsApplication eventsApplication ;

	private void addCommonAttributes ( ModelMap springViewModel ) {

		springViewModel.addAttribute( "csapPageLabel", "Csap Events Browser" ) ;
		springViewModel.addAttribute( "agentHostUrlPattern", eventsApplication.getHostUrlPattern( ) ) ;
		springViewModel.addAttribute( "maxHealthChangesPerDay", eventsApplication.getMaxHealthChangesPerDay( ) ) ;

	}

	@Inject
	HealthEventWriter healthEventWriter ;

	@RequestMapping ( "/testAsync" )
	@ResponseBody
	public String testAsync (
								@RequestParam ( value = "delaySeconds" , required = false , defaultValue = "5" ) int delaySeconds )
		throws Exception {

		String message = "Hello from " + this.getClass( ).getSimpleName( )
				+ " at " + LocalDateTime.now( ).format( DateTimeFormatter.ofPattern( "hh:mm:ss" ) ) ;
		healthEventWriter.printMessage( message, delaySeconds ) ;
		return "Look in logs for async to complete in: " + delaySeconds + " seconds" ;

	}

}
