package org.csap.events ;

import java.util.ArrayList ;
import java.util.List ;

import javax.inject.Inject ;

import org.apache.commons.lang3.StringUtils ;
import org.aspectj.lang.ProceedingJoinPoint ;
import org.aspectj.lang.annotation.Around ;
import org.aspectj.lang.annotation.Aspect ;
import org.aspectj.lang.annotation.Pointcut ;
import org.csap.CsapBootApplication ;
import org.csap.events.admin.AdminDBHelper ;
import org.csap.events.db.AnalyticsHelper ;
import org.csap.events.db.EventDataHelper ;
import org.csap.events.db.GlobalAnalyticsDbReader ;
import org.csap.events.db.MetricsDataHandler ;
import org.csap.events.db.MetricsDataReader ;
import org.csap.events.db.ServiceAnalytics ;
import org.csap.events.db.TrendingReportHelper ;
import org.csap.events.monitoring.MongoClusterListener ;
import org.csap.events.monitoring.MongoCommandListener ;
import org.csap.helpers.CsapApplication ;
import org.csap.integations.CsapEncryptionConfiguration ;
import org.csap.integations.CsapMicroMeter ;
import org.csap.integations.CsapSecurityConfiguration ;
import org.csap.security.config.CsapSecurityRoles ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty ;
import org.springframework.boot.context.properties.ConfigurationProperties ;
import org.springframework.context.annotation.Bean ;
import org.springframework.core.env.Environment ;
import org.springframework.core.task.TaskExecutor ;
import org.springframework.http.HttpMethod ;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory ;
import org.springframework.http.converter.FormHttpMessageConverter ;
import org.springframework.http.converter.StringHttpMessageConverter ;
import org.springframework.scheduling.TaskScheduler ;
import org.springframework.scheduling.annotation.EnableAsync ;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor ;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler ;
import org.springframework.web.client.RestTemplate ;
import org.springframework.web.servlet.config.annotation.CorsRegistry ;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry ;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer ;
import org.springframework.web.servlet.resource.VersionResourceResolver ;

import com.fasterxml.jackson.databind.ObjectMapper ;
import com.mongodb.MongoClient ;
import com.mongodb.MongoClientOptions ;
import com.mongodb.MongoCredential ;
import com.mongodb.ReadPreference ;
import com.mongodb.ServerAddress ;

@CsapBootApplication
@EnableAsync
@ConfigurationProperties ( CsapEventsApplication.CONFIGURATION_PREFIX )
@Aspect
public class CsapEventsApplication implements WebMvcConfigurer {

	final static public String CONFIGURATION_PREFIX = "csap-events" ;

	public final static String BASE_URL = "/" ;
	public final static String API_URL = BASE_URL + "api" ;
	public final static String JSP_VIEW = "/view/" ;

	public final static String ADMIN_API = API_URL + "/admin" ;
	public final static String EVENT_API = API_URL + "/event" ;
	public final static String REPORT_API = API_URL + "/report" ;
	public final static String HEALTH_API = API_URL + "/health" ;
	public final static String METRICS_API = API_URL + "/metrics" ;
	public final static String ATTRIBUTES_CACHE = "attributesCache" ;
	public final static String SIMPLE_REPORT_CACHE = "simpleReportCache" ;
	public final static String DETAILS_REPORT_CACHE = "detailsReportCache" ;
	public final static String METRICS_REPORT_CACHE = "metricsReportCache" ;
	public final static String HISTORICAL_REPORT_CACHE = "historicalPerformanceCache" ;
	public final static String NUM_DAYS_CACHE = "numDaysCache" ;

	final static private int ONE_YEAR_SECONDS = 60 * 60 * 24 * 365 ;

	private int maxHealthChangesPerDay = 21 ;

	final Logger logger = LoggerFactory.getLogger( getClass( ) ) ;
	private MongoConfig mongoConfig ;

	@Inject
	Environment env ;

	@Inject
	ObjectMapper jsonMapper ;

	private String hostUrlPattern = "http://CSAP_HOST.yourcompany.com:8011" ;

	public String getHostUrlPattern ( ) {

		return hostUrlPattern ;

	}

	public void setHostUrlPattern ( String hostUrlPattern ) {

		this.hostUrlPattern = hostUrlPattern ;

	}

	public static void main ( String[] args ) {

		CsapApplication.run( CsapEventsApplication.class, args ) ;

		// SpringApplication.run( CsapDataApplication.class, args );
	}

	public enum SimonIds {

		// add as many as needed. Optionally read values
		// from limits.yml
		exceptions("health.exceptions"), nullPointer("health.nullPointer");

		public String id ;

		private SimonIds ( String simonId ) {

			this.id = simonId ;

		}
	}

	// configure @Scheduled thread pool
	@Bean
	public TaskScheduler taskScheduler ( ) {

		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler( ) ;
		scheduler.setThreadNamePrefix( CsapEventsApplication.class.getSimpleName( ) + "Scheduler" ) ;
		scheduler.setPoolSize( 2 ) ;
		return scheduler ;

	}

	// configure @Async thread pool
	final public static String HEALTH_EXECUTOR = "CsapHealthExecutor" ;

	@Bean ( HEALTH_EXECUTOR )
	public TaskExecutor taskExecutor ( ) {

		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor( ) ;
		taskExecutor.setThreadNamePrefix( CsapEventsApplication.class.getSimpleName( ) + "@Async" ) ;
		taskExecutor.setMaxPoolSize( 5 ) ;
		taskExecutor.setQueueCapacity( 300 ) ;
		taskExecutor.afterPropertiesSet( ) ;
		return taskExecutor ;

	}

	MongoCredential mongoCredential = null ;

	public MongoCredential getMongoCredential ( ) {

		return mongoCredential ;

	}

	final static int MAX_MONGO_IDLE_MS = 60 * 1000 ;

	@Bean
	public MongoClient getMongoClient ( CsapEncryptionConfiguration csapEnc )
		throws Exception {

		logger.info( "{} \n\t DBMax Idle Seconds: {}",
				mongoConfig.toString( ).replaceAll( ",", ",\n" ),
				MAX_MONGO_IDLE_MS / 1000 ) ;

		MongoClient client = null ;

		try {

			String[] mongoHosts = getMongoConfig( ).getHosts( ).split( " " ) ;
			List<ServerAddress> serverList = new ArrayList<>( ) ;

			for ( String host : mongoHosts ) {

				ServerAddress serverAddress = new ServerAddress( host, getMongoConfig( ).getPort( ) ) ;
				serverList.add( serverAddress ) ;

			}

			List<MongoCredential> credentialList = new ArrayList<>( ) ;

			if ( ! StringUtils.isEmpty( getMongoConfig( ).getUser( ) ) ) {

				String password = csapEnc.decodeIfPossible( getMongoConfig( ).getPassword( ), logger ) ;

				mongoCredential = MongoCredential.createCredential( getMongoConfig( ).getUser( ),
						getMongoConfig( ).getUserDb( ), password.toCharArray( ) ) ;

				credentialList.add( mongoCredential ) ;

			}

			MongoClientOptions options = MongoClientOptions.builder( ).addCommandListener( new MongoCommandListener( ) )
					.addClusterListener( new MongoClusterListener( ReadPreference.nearest( ) ) )
					.maxConnectionIdleTime( MAX_MONGO_IDLE_MS ).build( ) ;

			client = new MongoClient( serverList, credentialList, options ) ;

		} catch ( Exception e ) {

			logger.error( "Exception while connecting to mongodb.", e ) ;

		}

		if ( null != client ) {

			logger.info( "Mongo Connection Options" +
					client.getMongoClientOptions( ).toString( ).replaceAll( ",", ",\n" ) ) ;

		}

		return client ;

	}

	static int mongoTestPort = -1 ;

	public static int getMongoTestPort ( ) {

		return mongoTestPort ;

	}

	public static void setMongoTestPort ( int mongoTestPort ) {

		CsapEventsApplication.mongoTestPort = mongoTestPort ;

	}

	public MongoConfig getMongoConfig ( ) {

		if ( getMongoTestPort( ) >= 0 && mongoConfig != null ) {

			mongoConfig.setPort( getMongoTestPort( ) ) ;

		}

		return mongoConfig ;

	}

	public void setMongoConfig ( MongoConfig mongoConfig ) {

		this.mongoConfig = mongoConfig ;

	}

	@Bean
	@ConditionalOnProperty ( "csap.security.enabled" )
	public CsapSecurityConfiguration.CustomHttpSecurity security_policy_for_event ( ) {

		// ref
		// https://docs.spring.io/spring-security/site/docs/3.0.x/reference/el-access.html
		// String rootAcl="hasRole('ROLE_AUTHENTICATED')";

		// @formatter:off
		return ( httpSecurity -> {

			httpSecurity
					// CSRF adds complexity - refer to
					// https://docs.spring.io/spring-security/site/docs/current/reference/htmlsingle/#csrf
					// csap.security.csrf also needs to be enabled or this will be ignored
					.csrf( )
					.requireCsrfProtectionMatcher( CsapSecurityConfiguration.buildRequestMatcher( "/login*" ) )
					.and( )

					.authorizeRequests( )

					// protect using csap api filter
					.antMatchers( "/api/**" )
					.permitAll( )
					//
					//
					.antMatchers( "/webjars/**", "/noAuth/**", "/js/**", "/css/**", "/images/**" )
					.permitAll( )
					// protect admin operations
					//
					.antMatchers( EVENT_API + "/delete/**", EVENT_API + "/insert/**",
							EVENT_API + "/deleteBySearch/**", EVENT_API + "/update/**" )
					.access( CsapSecurityRoles.hasAny( CsapSecurityRoles.Access.admin ) )
					//
					//
					.anyRequest( )
					.access( CsapSecurityRoles.hasAny( CsapSecurityRoles.Access.view ) ) ;

		} ) ;

		// @formatter:on

	}

	// https://spring.io/blog/2014/07/24/spring-framework-4-1-handling-static-web-resources
	// http://www.mscharhag.com/spring/resource-versioning-with-spring-mvc
	@Override
	public void addResourceHandlers ( ResourceHandlerRegistry registry ) {

		if ( ! CsapApplication.isCsapFolderSet( ) ) {

			logger.warn( "\n\n\n Desktop detected: Caching DISABLED \n\n\n" ) ;
			return ;

		}

		String version = "start" + System.currentTimeMillis( ) ;
		VersionResourceResolver versionResolver = new VersionResourceResolver( )
				// .addFixedVersionStrategy( version, "/**/*.js" ) //Enable this
				// if we use a JavaScript module loader
				.addFixedVersionStrategy( version,
						"/**/modules/**/*.js" ) // requriesjs uses relative paths
				.addContentVersionStrategy( "/**" ) ;

		// A Handler With Versioning - note images in css files need to be
		// resolved.
		registry
				.addResourceHandler( "/**/*.js", "/**/*.css", "/**/*.png", "/**/*.gif", "/**/*.jpg" )
				.addResourceLocations( "classpath:/static/", "classpath:/public/" )
				// .addResourceLocations( "classpath:/analytics/" )
				.setCachePeriod( ONE_YEAR_SECONDS )
				.resourceChain( true )
				.addResolver( versionResolver ) ;

	}

	@Bean
	public WebMvcConfigurer corsConfigurer ( ) {

		return new WebMvcConfigurer( ) {
			@Override
			public void addCorsMappings ( CorsRegistry registry ) {

				registry.addMapping( "/api/**" ).allowedMethods( HttpMethod.GET.name( ) ) ;

			}
		} ;

	}

	@Bean
	public EventDataHelper getEventDataHelper ( ) {

		return new EventDataHelper( ) ;

	}

	/*
	 * @Bean public Jmx_EventDBMonitoring getJmx_EventDBMonitoring(){ return new
	 * Jmx_EventDBMonitoring(); }
	 */

	@Bean
	public AdminDBHelper getAdminDBHelper ( ) {

		return new AdminDBHelper( ) ;

	}

	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	@Pointcut ( "within(org.csap.events.http.ui.rest..*)" )
	private void restEventPC ( ) {

	}

	@Around ( "restEventPC()" )
	public Object eventMicroMeter ( ProceedingJoinPoint pjp )
		throws Throwable {

		return metricUtilities.timedExecution( pjp, "x-rest." ) ;

	}

	@Pointcut ( "within(org.csap.events.db..*)" )
	private void dbPC ( ) {

	}

	@Around ( "dbPC()" )
	public Object linuxAdvice ( ProceedingJoinPoint pjp )
		throws Throwable {

		return metricUtilities.timedExecution( pjp, "x-db." ) ;

	}

	@Pointcut ( "within(org.csap.events.health..*)" )
	private void healthPC ( ) {

	}

	@Around ( "healthPC()" )
	public Object healthPCAdvice ( ProceedingJoinPoint pjp )
		throws Throwable {

		return metricUtilities.timedExecution( pjp, "x-health." ) ;

	}

	@Pointcut ( "within(org.csap.events.monitoring..*)" )
	private void monitoringPC ( ) {

	}

	@Around ( "monitoringPC()" )
	public Object monitoringPCAdvice ( ProceedingJoinPoint pjp )
		throws Throwable {

		return metricUtilities.timedExecution( pjp, "x-health." ) ;

	}

	@Bean
	public TrendingReportHelper getTrendingReportHelper ( ) {

		return new TrendingReportHelper( ) ;

	}

	@Bean
	public GlobalAnalyticsDbReader getGlobalAnalyticsDbReader ( ) {

		return new GlobalAnalyticsDbReader( ) ;

	}

	@Bean
	public AnalyticsHelper getAnalyticsHelper ( ) {

		return new AnalyticsHelper( ) ;

	}

	@Bean
	public ServiceAnalytics getServiceAnalytics ( ) {

		return new ServiceAnalytics( ) ;

	}

	@Bean
	public MetricsDataReader getMetricsDataReader ( ) {

		return new MetricsDataReader( ) ;

	}

	@Bean
	public MetricsDataHandler getMetricsDataHandler ( ) {

		return new MetricsDataHandler( ) ;

	}

	@Bean ( name = "genericRestTemplate" )
	public RestTemplate getGenericRestTemplate ( ) {

		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory( ) ;
		// factory.setHttpClient(httpClient);
		// factory.getHttpClient().getConnectionManager().getSchemeRegistry().register(scheme);

		factory.setConnectTimeout( 120000 ) ;
		factory.setReadTimeout( 120000 ) ;

		RestTemplate restTemplate = new RestTemplate( factory ) ;
		restTemplate.getMessageConverters( ).clear( ) ;
		restTemplate.getMessageConverters( ).add( new FormHttpMessageConverter( ) ) ;
		restTemplate.getMessageConverters( ).add( new StringHttpMessageConverter( ) ) ;

		return restTemplate ;

	}

	public int getMaxHealthChangesPerDay ( ) {

		return maxHealthChangesPerDay ;

	}

	public void setMaxHealthChangesPerDay ( int maxHealthChangesPerDay ) {

		this.maxHealthChangesPerDay = maxHealthChangesPerDay ;

	}
}
