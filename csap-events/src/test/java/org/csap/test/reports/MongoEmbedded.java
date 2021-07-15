package org.csap.test.reports ;

import java.io.File ;
import java.text.SimpleDateFormat ;
import java.util.Calendar ;
import java.util.Date ;
import java.util.HashMap ;
import java.util.Map ;
import java.util.concurrent.TimeUnit ;

import org.apache.commons.io.FileUtils ;
import org.bson.Document ;
import org.csap.events.CsapEventsApplication ;
import org.csap.events.util.MongoConstants ;
import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapApplication ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.context.event.ContextClosedEvent ;
import org.springframework.context.event.EventListener ;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory ;
import org.springframework.http.converter.FormHttpMessageConverter ;
import org.springframework.http.converter.StringHttpMessageConverter ;
import org.springframework.stereotype.Service ;
import org.springframework.web.client.RestTemplate ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ArrayNode ;
import com.mongodb.BasicDBObject ;
import com.mongodb.MongoClient ;
import com.mongodb.MongoClientOptions ;
import com.mongodb.ServerAddress ;
import com.mongodb.client.MongoCollection ;
import com.mongodb.client.MongoDatabase ;

import de.flapdoodle.embed.mongo.Command ;
import de.flapdoodle.embed.mongo.MongoImportExecutable ;
import de.flapdoodle.embed.mongo.MongoImportProcess ;
import de.flapdoodle.embed.mongo.MongoImportStarter ;
import de.flapdoodle.embed.mongo.MongodExecutable ;
import de.flapdoodle.embed.mongo.MongodProcess ;
import de.flapdoodle.embed.mongo.MongodStarter ;
import de.flapdoodle.embed.mongo.config.Defaults ;
import de.flapdoodle.embed.mongo.config.MongoImportConfig ;
import de.flapdoodle.embed.mongo.config.MongodConfig ;
import de.flapdoodle.embed.mongo.config.Net ;
import de.flapdoodle.embed.mongo.config.Storage ;
import de.flapdoodle.embed.mongo.distribution.Version ;
import de.flapdoodle.embed.process.extract.UUIDTempNaming ;
import de.flapdoodle.embed.process.io.directories.FixedPath ;
import de.flapdoodle.embed.process.runtime.Network ;

@Service
public class MongoEmbedded {

	Logger logger = LoggerFactory.getLogger( MongoEmbedded.class ) ;

	final String MONGO_TEST_DIR = "./target/flapdoodle" ;
	final String MONGO_TEST_STORAGE_LOCATION = MONGO_TEST_DIR + "/storage" ;
	final String DB_FILE = "events.json" ;
	final String DATE = "02/01/2019" ;

	MongodStarter starter = null ; // MongodStarter.getDefaultInstance();
	MongodExecutable mongodExe ;
	MongodProcess mongod ;
	MongoClient mongoClient ;
	MongoImportProcess mongoImport ;

	private int mongoServerPort ;

	private ObjectMapper jacksonMapper ;

	@Autowired
	public MongoEmbedded ( ObjectMapper jacksonMapper ) {

		this.jacksonMapper = jacksonMapper ;

		try {

			mongoServerPort = Network.getFreeServerPort( ) ;

		} catch ( Exception e ) {

			logger.error( "Failed to create port {}", CSAP.buildCsapStack( e ) ) ;

		}

		CsapEventsApplication.setMongoTestPort( mongoServerPort ) ;

		// MONGO_TEST_PORT = 12345 ; // client hardcoded in app-junit.yml,
		setUpMongo( ) ;

	}

	// @PostConstruct
	public void setUpMongo ( ) {

		try {

			logger.info( CsapApplication.testHeader( ) ) ;

			var previousStorage = new File( MONGO_TEST_STORAGE_LOCATION ) ;

			// Delete storage in case dir is still around
			if ( previousStorage.exists( ) ) {

				if ( ! FileUtils.deleteQuietly( previousStorage ) ) {

					logger.warn( "Unable to delete: {}", previousStorage.getAbsolutePath( ) ) ;

				}

			}

			//
			// STEP 1: Download the binaries
			//

			var mongodCommand = Command.MongoD ;
			var artifactStorePath = new FixedPath( MONGO_TEST_STORAGE_LOCATION ) ;

			if ( FlapMongo.reUseDownload ) {

				artifactStorePath = new FixedPath( System.getProperty( "user.home" ) + "/" + getClass( ).getName( ) ) ;

			}

			var executableNaming = new UUIDTempNaming( ) ;

			var runtimeConfig = Defaults.runtimeConfigFor( mongodCommand )

					.artifactStore(
							Defaults.extractedArtifactStoreFor( mongodCommand )
									.withDownloadConfig( Defaults.downloadConfigFor( mongodCommand )
											.artifactStorePath( artifactStorePath )
											.build( ) )
									.executableNaming( executableNaming ) )
					.build( ) ;

			logger.info( CsapApplication.header( "Downloading mongo to: {}" ), artifactStorePath.asFile( ) ) ;
			MongodStarter runtime = MongodStarter.getInstance( runtimeConfig ) ;

			//
			// Step 2: configure and start an instance
			//
			var databaseDir = new Storage( MONGO_TEST_STORAGE_LOCATION, null, 0 ) ;
			var mongodConfig = MongodConfig.builder( )
					.version( Version.Main.V4_0 )
					.replication( databaseDir )
					.net( new Net( mongoServerPort, Network.localhostIsIPv6( ) ) )
					.build( ) ;

			mongodExe = runtime.prepare( mongodConfig ) ;

			logger.info( CsapApplication.header(
					"starting mongo \n\t mongoPort: {} \n\t db storage: {},  \n\t executable: {} " ),
					mongoServerPort,
					databaseDir.getDatabaseDir( ),
					mongodExe.getFile( ).executable( ) ) ;

			mongod = mongodExe.start( ) ;

			logger.info( "mongo running: {}", mongod.isProcessRunning( ) ) ;

			TimeUnit.SECONDS.sleep( 3 ) ;

		} catch ( Exception e ) {

			logger.error( "Failed to download and start mongo {}", CSAP.buildCsapStack( e ) ) ;
			logger.debug( "Full stack", e ) ;

		}

		try {

			ServerAddress serverAddress = new ServerAddress( "localhost", mongoServerPort ) ;

			logger.info( CsapApplication.testHeader( "creating connection: {}" ), serverAddress ) ;

			var connectOptions = MongoClientOptions.builder( )
					.connectTimeout( 2000 )
					.socketTimeout( 2000 )
					.maxWaitTime( 2000 )
					.serverSelectionTimeout( 1000 )
					.build( ) ;

			mongoClient = new MongoClient( serverAddress, connectOptions ) ;

			TimeUnit.SECONDS.sleep( 1 ) ;
			createUser( ) ;

			TimeUnit.SECONDS.sleep( 1 ) ;
			String file = getResouceFile( DB_FILE ) ;
			// mongoImport = startMongoImport( MONGO_TEST_PORT, DB_NAME, DB_COLLECTION,
			// file, true, true, true ) ;
			reloadDefaultEventData( ) ;

		} catch ( Exception e ) {

			logger.error( "Failed to Import test data from {} {}", DB_FILE, CSAP.buildCsapStack( e ) ) ;

		}

	}

	// note @PreDestroy - logger is already dead. Use Spring lc instead.
	@EventListener
	public void onApplicationEvent ( ContextClosedEvent event ) {

		try {

			logger.info( CsapApplication.testHeader( "Stopping Mongo and removing storage: {}  " ),
					MONGO_TEST_STORAGE_LOCATION ) ;

//			mongod.stop( ) ;
			mongodExe.stop( ) ;

			for ( var i = 0; i < 10; i++ ) {

				if ( mongod.isProcessRunning( ) ) {

					TimeUnit.SECONDS.sleep( 1 ) ;
					logger.info( "Mongo shutdown in progress..." ) ;

				} else {

					break ;

				}

			}

			FileUtils.deleteQuietly( new File( MONGO_TEST_STORAGE_LOCATION ) ) ;

		} catch ( Exception e ) {

			logger.error( "Errors during shut down {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	private String getResouceFile ( String fileName )
		throws Exception {

		ClassLoader classloader = getClass( ).getClassLoader( ) ;
		String file = classloader.getResource( fileName ).getFile( ) ;
		logger.info( "Loading file: " + file ) ;

		return file ;

	}

	private RestTemplate getGenericRestTemplate ( ) {

		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory( ) ;
		factory.setConnectTimeout( 120000 ) ;
		factory.setReadTimeout( 120000 ) ;

		RestTemplate restTemplate = new RestTemplate( factory ) ;
		restTemplate.getMessageConverters( ).clear( ) ;
		restTemplate.getMessageConverters( ).add( new FormHttpMessageConverter( ) ) ;
		restTemplate.getMessageConverters( ).add( new StringHttpMessageConverter( ) ) ;

		return restTemplate ;

	}

	void reloadDefaultEventData ( )
		throws Exception {

		var eventsFile = new File( getClass( ).getClassLoader( ).getResource( DB_FILE ).getPath( ) ) ;

		logger.info( CsapApplication.testHeader( "loading: {}" ), eventsFile.getAbsolutePath( ) ) ;

		var events = jacksonMapper.readTree( eventsFile ) ;

		MongoDatabase db = mongoClient.getDatabase( MongoConstants.EVENT_DB_NAME ) ;
		MongoCollection<Document> eventRecords = db.getCollection( MongoConstants.EVENT_COLLECTION_NAME ) ;

		eventRecords.drop( ) ;

		CSAP.jsonStream( events )
				.forEach( event -> {

					Document eventDoc = Document.parse( event.toString( ) ) ;
					eventRecords.insertOne( eventDoc ) ;
					;
					// eventDocument.put( APPID, appId ) ;
					// eventDocument.put( LIFE, life ) ;
					// eventDocument.put( PROJECT, project ) ;
					// eventDocument.put( HOST, getHostName() ) ;
					// eventDocument.put( CATEGORY, category ) ;
					// eventDocument.put( SUMMARY, summary ) ;

					// DBObject dbObject = (DBObject) JSON.parse( event.toString() ) ;
					//
					// eventRecords.insert( dbObject ) ;

				} ) ;

		logger.info( CsapApplication.testHeader( "loaded: {} events" ), eventRecords.count( ) ) ;

	}

	private MongoImportProcess startMongoImport (
													int port ,
													String dbName ,
													String collection ,
													String jsonFile ,
													Boolean jsonArray ,
													Boolean upsert ,
													Boolean drop )
		throws Exception {

		logger.info( CsapApplication.testHeader( "Running import: db: {} collection: {} port {}" ), dbName, collection,
				port ) ;

		var runtimeConfig = Defaults.runtimeConfigFor( Command.MongoImport ).build( ) ;

		MongoImportConfig mongoImportConfig = MongoImportConfig.builder( )
				.version( Version.Main.PRODUCTION )
				.net( new Net( port, Network.localhostIsIPv6( ) ) )
				.databaseName( dbName )
				.collectionName( collection )
				.isUpsertDocuments( true )
				.isDropCollection( true )
				.isJsonArray( true )
				.importFile( jsonFile )
				.build( ) ;

//		IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder( )
//				// .defaultsWithLogger( Command.MongoD, mongoLogger )
//				.defaults( Command.MongoImport )
//				.build( ) ;
//
//		IMongoImportConfig mongoImportConfig = new MongoImportConfigBuilder( )
//				.version( Version.V4_0_2 )
//				.net( new Net( MONGO_TEST_PORT, Network.localhostIsIPv6( ) ) )
//				.db( dbName )
//				.collection( collection )
//				.upsert( upsert )
//				.dropCollection( drop )
//				.jsonArray( jsonArray )
//				.importFile( jsonFile )
//				.build( ) ;

		MongoImportExecutable mongoImportExecutable = MongoImportStarter.getDefaultInstance( )
				.prepare( mongoImportConfig ) ;

		logger.info( "File: {}", mongoImportExecutable.getFile( ) ) ;

		MongoImportProcess mongoImport = mongoImportExecutable.start( ) ;

		while ( mongoImport.isProcessRunning( ) ) {

			Thread.sleep( 1000 ) ;

		}

		return mongoImport ;

	}

	/**
	 * CSAP is using old authentication scheme
	 */
	private void createUser ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		MongoDatabase db = mongoClient.getDatabase( "admin" ) ;
		Document authVersionDoc = new Document( "_id", "authSchema" ) ;
		authVersionDoc.append( "currentVersion", 5 ) ;
		mongoClient.getDatabase( "admin" ).getCollection( "system.version" ).insertOne( authVersionDoc ) ;

		Map<String, Object> commandArguments = new BasicDBObject( ) ;
		commandArguments.put( "createUser", "dataBaseReadWriteUser" ) ;
		commandArguments.put( "pwd", "password" ) ;
		String[] roles = {
				"dbAdminAnyDatabase", "readWriteAnyDatabase", "clusterAdmin", "userAdminAnyDatabase"
		} ;
		commandArguments.put( "roles", roles ) ;
		BasicDBObject command = new BasicDBObject( commandArguments ) ;
		Document result = db.runCommand( command ) ;

	}

	public int getDateOffSet ( String startDate )
		throws Exception {

		Calendar today = Calendar.getInstance( ) ;
		int diffInDays = (int) ( ( today.getTimeInMillis( ) - ( getStartDate( startDate ) ).getTime( ) )
				/ ( 1000 * 60 * 60 * 24 ) ) ;
		logger.debug( "Date offset {} ", diffInDays ) ;
		return diffInDays ;

	}

	private Date getStartDate ( String dateString )
		throws Exception {

		SimpleDateFormat sdf = new SimpleDateFormat( "MM/dd/yyyy" ) ;
		// sdf.setTimeZone(TimeZone.getTimeZone("CST"));
		Date date = sdf.parse( dateString ) ;
		return date ;

	}

	public Map<String, String> createStandardUrlParamMap ( String numDays )
		throws Exception {

		Map<String, String> urlParams = new HashMap<>( ) ;
		urlParams.put( "appId", "SensusCsap" ) ;
		urlParams.put( "life", "dev" ) ;
		urlParams.put( "project", "CSAP Platform" ) ;
		urlParams.put( "numDays", numDays ) ;
		urlParams.put( "dateOffSet", "" + getDateOffSet( DATE ) ) ;
		return urlParams ;

	}

	public JsonNode getJsonNodeFromArray ( JsonNode dataNode , String attributeName , String attributeValue ) {

		if ( dataNode.isArray( ) ) {

			ArrayNode arrayDataNode = (ArrayNode) dataNode ;

			for ( int i = 0; i < arrayDataNode.size( ); i++ ) {

				JsonNode dataElementNode = arrayDataNode.get( i ) ;

				if ( attributeValue.equals( dataElementNode.at( "/" + attributeName ).asText( ) ) ) {

					return dataElementNode ;

				}

			}

		}

		return jacksonMapper.createObjectNode( ) ;

	}

}
