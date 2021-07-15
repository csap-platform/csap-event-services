package org.csap.events ;

import javax.annotation.PostConstruct ;

import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapApplication ;
import org.csap.integations.CsapEncryptionConfiguration ;
import org.csap.integations.CsapInformation ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.boot.context.properties.ConfigurationProperties ;
import org.springframework.context.annotation.Configuration ;
import org.springframework.http.HttpHeaders ;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory ;
import org.springframework.http.converter.FormHttpMessageConverter ;
import org.springframework.http.converter.StringHttpMessageConverter ;
import org.springframework.util.LinkedMultiValueMap ;
import org.springframework.util.MultiValueMap ;
import org.springframework.web.client.RestTemplate ;

/**
 *
 * @author paranant
 */
@Configuration
@ConfigurationProperties ( CsapEventsApplication.CONFIGURATION_PREFIX + ".data-connection" )
public class EventServiceConfiguration {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	private String url ;
	private String pass ;
	private String user ;

	@Autowired
	public EventServiceConfiguration ( CsapInformation csapInformation, CsapEncryptionConfiguration csapEnc ) {

		this.csapInformation = csapInformation ;
		this.csapEnc = csapEnc ;

	}

	@PostConstruct
	void printVals ( ) {

		logger.warn( CsapApplication.header( "dataServiceUrl : '{}' (add in configmap to override)  \n user: '{}'" ),
				getUrl( ),
				getUser( ) ) ;

	}

	private CsapInformation csapInformation ;
	CsapEncryptionConfiguration csapEnc ;

	public String postEventData ( String jsonDoc ) {

		StringBuilder response = new StringBuilder( "Event Post Response: \n" ) ;

		String eventServiceUrl = getUrl( ) ;
		response.append( "\n" + eventServiceUrl ) ;

		try {

			HttpHeaders headers = new HttpHeaders( ) ;
			headers.setContentType( org.springframework.http.MediaType.APPLICATION_JSON ) ;

			MultiValueMap<String, String> requestObj = new LinkedMultiValueMap<String, String>( ) ;
			requestObj.add( "userid", user ) ;
			requestObj.add( "pass", pass ) ;
			requestObj.add( "eventJson", jsonDoc ) ;

			logger.debug( "Posting to url: {} \n\t Data: {}", eventServiceUrl, requestObj ) ;
			String result = getRestPostTemplate( ).postForObject( eventServiceUrl, requestObj, String.class ) ;
			logger.debug( "result{}", result ) ;

		} catch ( Exception e ) {

			String stack = CSAP.buildCsapStack( e ) ;
			response.append( "\n Failed to post data: " + stack ) ;
			logger.error( "\n Failed to post event. user: '{}', '{}', \n url: {}, \n reason: {}, \n\n document: {}",
					user, pass, eventServiceUrl,
					stack, jsonDoc ) ;

		}

		return response.toString( ) ;

	}

	public RestTemplate getRestPostTemplate ( ) {

		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory( ) ;
		// factory.setHttpClient(httpClient);
		// factory.getHttpClient().getConnectionManager().getSchemeRegistry().register(scheme);

		factory.setConnectTimeout( 20000 ) ;
		factory.setReadTimeout( 20000 ) ;

		RestTemplate restTemplate = new RestTemplate( factory ) ;
		restTemplate.getMessageConverters( ).clear( ) ;
		restTemplate.getMessageConverters( ).add( new FormHttpMessageConverter( ) ) ;
		restTemplate.getMessageConverters( ).add( new StringHttpMessageConverter( ) ) ;

		return restTemplate ;

	}

	public void setPass ( String pass ) {

		String password = csapEnc.decodeIfPossible( pass, logger ) ;
		logger.debug( "Password is: {}", password ) ;
		this.pass = password ;

	}

	public String getPass ( ) {

		return pass ;

	}

	public String getUser ( ) {

		return user ;

	}

	public void setUser ( String user ) {

		this.user = user ;

	}

	/**
	 * @return the url
	 */
	public String getUrl ( ) {

		String targetUrl = url ;

		if ( ! url.startsWith( "http" ) ) {

			targetUrl = csapInformation.getLoadBalancerUrl( ) + url ;

		}

		return targetUrl ;

	}

	/**
	 * @param url the url to set
	 */
	public void setUrl ( String url ) {

		this.url = url ;

	}

	@Override
	public String toString ( ) {

		return "EventServiceConfiguration [url=" + url + ", pass=" + pass + ", user=" + user + "]" ;

	}

}
