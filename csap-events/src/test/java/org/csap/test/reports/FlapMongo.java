package org.csap.test.reports ;

import static org.assertj.core.api.Assertions.assertThat ;

import java.io.File ;
import java.util.Map ;
import java.util.concurrent.TimeUnit ;

import org.apache.commons.io.FileUtils ;
import org.bson.Document ;
import org.csap.events.util.MongoConstants ;
import org.csap.helpers.CSAP ;
import org.csap.helpers.CsapApplication ;
import org.junit.jupiter.api.AfterAll ;
import org.junit.jupiter.api.Tag ;
import org.junit.jupiter.api.Test ;
import org.junit.jupiter.api.TestInstance ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.fasterxml.jackson.databind.ObjectMapper ;
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
import de.flapdoodle.embed.process.config.io.ProcessOutput ;
import de.flapdoodle.embed.process.extract.UUIDTempNaming ;
import de.flapdoodle.embed.process.io.ConsoleOutputStreamProcessor ;
import de.flapdoodle.embed.process.io.Processors ;
import de.flapdoodle.embed.process.io.directories.Directory ;
import de.flapdoodle.embed.process.io.directories.FixedPath ;
import de.flapdoodle.embed.process.runtime.Network ;

@TestInstance ( TestInstance.Lifecycle.PER_CLASS )
@Tag ( "mongo" )
public class FlapMongo {

	Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	static public boolean reUseDownload = true ;

	Directory mongoStorage ;

	String MONGO_TEST_DIR = "./target/flapdoodle" ;
//	String MONGO_TEST_DIR = "/flapdoodle" ;
	String MONGO_TEST_STORAGE_LOCATION = MONGO_TEST_DIR + "/junit-storage" ;
	int mongoPort = 0 ;
	String DB_FILE = "events.json" ;

	ObjectMapper jsonMapper = new ObjectMapper( ) ;
	MongodStarter starter ;
	MongodExecutable mongodExe ;
	MongodProcess mongod ;
	MongoClient mongoClient ;
	MongoImportProcess mongoImportProcess ;

	public FlapMongo ( ) {

		try {

			mongoPort = Network.getFreeServerPort( ) ;

		} catch ( Exception e ) {

			logger.error( "Failed to create port {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	@Test
	void verify_flapMongo ( ) {

		var testLocation = new File( MONGO_TEST_STORAGE_LOCATION ) ;
		logger.info( CsapApplication.testHeader( "Deleting mongo location if it exists: {}" ), testLocation ) ;
		FileUtils.deleteQuietly( testLocation ) ;

		try {

			createMongod( ) ;

			createMongoClient( ) ;

			createMongoAdminUser( ) ;

			mongoClientImport( ) ;

		} catch ( Exception e ) {

			// TODO Auto-generated catch block
			logger.error( "Failed to create mongo {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	@AfterAll
	void tearDown ( ) throws Exception {
//		if ( mongod != null ) {

//			mongod.stop( ) ;
//		}

		if ( mongodExe != null ) {

			logger.info( CsapApplication.testHeader( "shutting mongo down" ) ) ;
			mongodExe.stop( ) ;

		}

	}

	void createMongod ( )
		throws Exception {

		logger.info( CsapApplication.testHeader( ) ) ;

		//
		// STEP 1: Download the binaries
		//
		var command = Command.MongoD ;

		var artifactStorePath = new FixedPath( MONGO_TEST_STORAGE_LOCATION ) ;

		if ( reUseDownload ) {

			artifactStorePath = new FixedPath( System.getProperty( "user.home" ) + "/" + getClass( ).getName( ) ) ;

		}

		var executableNaming = new UUIDTempNaming( ) ;

		var runtimeConfig = Defaults.runtimeConfigFor( command )

//				.processOutput( buildLogger( ) )

				.artifactStore(
						Defaults.extractedArtifactStoreFor( command )
								.withDownloadConfig( Defaults.downloadConfigFor( command )
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
		MongodConfig mongodConfig = MongodConfig.builder( )
//				.version( Version.Main.PRODUCTION )
//				.version( Version.V4_4_1 )
				.version( Version.Main.V4_0 )
				.replication( databaseDir )
				.net( new Net( mongoPort, Network.localhostIsIPv6( ) ) )
				.build( ) ;

		mongodExe = runtime.prepare( mongodConfig ) ;

		logger.info( CsapApplication.header(
				"starting mongo \n\t mongoPort: {} \n\t db storage: {},  \n\t executable: {} " ),
				mongoPort,
				databaseDir.getDatabaseDir( ),
				mongodExe.getFile( ).executable( ) ) ;

		mongod = mongodExe.start( ) ;

		logger.info( "mongo running: {}", mongod.isProcessRunning( ) ) ;

		TimeUnit.SECONDS.sleep( 2 ) ;

	}

	void mongoClientImport ( )
		throws Exception {

		var eventsFile = new File( getClass( ).getClassLoader( ).getResource( DB_FILE ).getPath( ) ) ;

		logger.info( CsapApplication.testHeader( "loading: {}" ), eventsFile.getAbsolutePath( ) ) ;

		var events = jsonMapper.readTree( eventsFile ) ;

		MongoDatabase db = mongoClient.getDatabase( MongoConstants.EVENT_DB_NAME ) ;
		MongoCollection<Document> eventRecords = db.getCollection( MongoConstants.EVENT_COLLECTION_NAME ) ;

		var beforeCount = eventRecords.count( ) ;
		logger.info( "Event records before: {}", beforeCount ) ;
		assertThat( beforeCount ).isEqualTo( 0 ) ;

		CSAP.jsonStream( events )
				.forEach( event -> {

					Document eventDoc = Document.parse( event.toString( ) ) ;
					eventRecords.insertOne( eventDoc ) ;

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

		var afterCount = eventRecords.count( ) ;
		logger.info( "Event records after: {}", afterCount ) ;

		assertThat( afterCount ).isEqualTo( 43 ) ;

	}

	ProcessOutput buildLogger ( ) {

		Logger mongoLogger = LoggerFactory.getLogger( "org.csap.test.flapWrapper" ) ;

		var console = new ConsoleOutputStreamProcessor( ) ;
		ProcessOutput processOutput = new ProcessOutput( console, console, Processors.named( "[console>]", console ) ) ;
//		ProcessOutput processOutput = new ProcessOutput(
//				Processors.logTo( mongoLogger, Slf4jLevel.INFO ),
//				
//				Processors.logTo( mongoLogger,
//						Slf4jLevel.ERROR ),
//
//				Processors.named( "[console>]", new ConsoleOutputStreamProcessor( ) ) ) ;
//		Processors.named( "[console>]",
//				Processors.logTo( mongoLogger, Slf4jLevel.DEBUG ) ) ) ;
		return processOutput ;

	}

	void mongoImportUsingCommand ( )
		throws Exception {

		String eventsPath = getClass( ).getClassLoader( ).getResource( DB_FILE ).toURI( ).toURL( ).getFile( ) ;

		logger.info( CsapApplication.testHeader( "loading: {}" ), eventsPath ) ;

		var runtimeConfig = Defaults.runtimeConfigFor( Command.MongoImport )

//				.processOutput( buildLogger( ) )

				.build( ) ;

		MongoImportConfig mongoImportConfig = MongoImportConfig.builder( )
				.version( Version.V4_4_1 )
				.net( new Net( mongoPort, Network.localhostIsIPv6( ) ) )
				.databaseName( MongoConstants.EVENT_DB_NAME )
				.collectionName( MongoConstants.EVENT_COLLECTION_NAME )
				.isUpsertDocuments( true )
				.isDropCollection( true )
				.isJsonArray( true )
				.importFile( eventsPath )
				.build( ) ;

//		IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder( )
//				// .defaultsWithLogger( Command.MongoD, mongoLogger )
//				.defaults( Command.MongoImport )
//				.artifactStore( mongoArtifactStore( Command.MongoImport ) )
//				.build( ) ;
//
//		IMongoImportConfig mongoImportConfig = new MongoImportConfigBuilder( )
//				.version( Version.V4_0_2 )
//				.net( new Net( mongoPort, Network.localhostIsIPv6( ) ) )
//				.db( MongoConstants.EVENT_DB_NAME )
//				.collection( MongoConstants.EVENT_COLLECTION_NAME )
//				.upsert( true )
//				.dropCollection( true )
//				.jsonArray( true )
//				.importFile( eventsPath )
//				.build( ) ;

		logger.info( "Creating executable" ) ;
		MongoImportExecutable mongoImportExecutable = MongoImportStarter
				// .getDefaultInstance()
				.getInstance( runtimeConfig )
				.prepare( mongoImportConfig ) ;

		logger.info( "File: {}", mongoImportExecutable.getFile( ).executable( ) ) ;

		mongoImportProcess = mongoImportExecutable.start( ) ;

		while ( mongoImportProcess.isProcessRunning( ) ) {

			Thread.sleep( 1000 ) ;

		}

	}

	void createMongoAdminUser ( )
		throws Exception {

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

		logger.info( CsapApplication.header( "admin user create result: {}" ), result.toString( ) ) ;

		assertThat( result.getDouble( "ok" ) ).isEqualTo( 1.0 ) ;

		TimeUnit.SECONDS.sleep( 1 ) ;

	}

	void createMongoClient ( )
		throws Exception {

		ServerAddress serverAddress = new ServerAddress( "localhost", mongoPort ) ;

		logger.info( CsapApplication.testHeader( "creating connection: {}" ), serverAddress ) ;

		var connectOptions = MongoClientOptions.builder( )
				.connectTimeout( 2000 )
				.socketTimeout( 2000 )
				.maxWaitTime( 2000 )
				.serverSelectionTimeout( 1000 )
				.build( ) ;

		mongoClient = new MongoClient( serverAddress, connectOptions ) ;

//		mongoClient = new MongoClient( "localhost", mongoPort ) ;

	}

//	private ArtifactStoreBuilder mongoArtifactStore ( Command mongoCommand ) {
//
//		var mongoStorage = new ExtractedArtifactStoreBuilder( )
//				.defaults( mongoCommand )
//				.extractDir( new FixedPath( MONGO_TEST_DIR ) )
//				// .tempDir( new FixedPath( MONGO_TEST_DIR + "/temp" ) )
//				.download( new DownloadConfigBuilder( )
//						.defaultsForCommand( mongoCommand )
//						.artifactStorePath( new FixedPath( MONGO_TEST_DIR ) )
//				// .downloadPath("http://csaptools.yourcompany.com/mongo/")
//				) ;
//		return mongoStorage ;
//	}

}
