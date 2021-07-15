package org.csap.events ;

import java.io.IOException ;
import java.util.Arrays ;
import java.util.List ;

import org.bson.Document ;
import org.bson.json.JsonMode ;
import org.bson.json.JsonWriterSettings ;

import com.fasterxml.jackson.databind.JsonNode ;
import com.fasterxml.jackson.databind.ObjectMapper ;
import com.fasterxml.jackson.databind.node.ArrayNode ;
import com.mongodb.client.AggregateIterable ;

public class EventJsonConstants {

	// Used by https://datatables.net/manual/server-side
	public static final String DATA_TABLES_RECORDS_TOTAL = "recordsTotal" ;
	public static final String DATA_TABLES_RECORDS_FILTERED = "recordsFiltered" ;

	public static final String CSAP_AGENT = "csap-agent" ;

	// Prevent long running request from overwhelming system
	public static final int MAX_QUERY_TIME_SECONDS = 10 ;

	public static final String EVENT_ALL_INDEX = "appId_1_project_1_lifecycle_1_category_1_createdOn.date_-1" ;
	public static final String EVENT_DATE_INDEX = "createdOn.date_-1" ;

	public static final String CATEGORY = "category" ;
	public static final String PROJECT = "project" ;
	public static final String $PROJECT = "$" + PROJECT ;
	public static final int INCLUDE_FIELD = 1 ;
	public static final String HOST = "host" ;
	public static final String CREATED_ON = "createdOn" ;
	public static final String CREATED_ON_DATE = "createdOn.date" ;
	public static final String LAST_UPDATED_ON = "lastUpdatedOn" ;
	public static final String CREATED_ON_LAST_UPDATED = CREATED_ON + "." + LAST_UPDATED_ON ;
	public static final String DATE = "date" ;
	public static final String TIME = "time" ;
	public static final String UNIXMS = "unixMs" ;
	public static final String MONGO_DATE = "mongoDate" ;
	public static final String APPID = "appId" ;
	public static final String $APPID = "$" + APPID ;
	public static final String _ID = "_id" ;
	public static final String LIFE = "lifecycle" ;
	public static final String $LIFE = "$" + LIFE ;
	public static final String SUMMARY = "summary" ;
	public static final String UI_USER = "metaData.uiUser" ;
	public static final String UIUSER = "uiUser" ;
	public static final String DATA = "data" ;
	public static final String EXPIRES_AT = "expiresAt" ;
	public static final String CREATED_ON_MONGO_DATE = CREATED_ON + "." + MONGO_DATE ;
	public static final String DATA_KEY = "dataKey" ;
	public static final String PAYLOAD_CATEGORY = "/csap/exception/payload" ;
	public static final String METRICS_ATTRIBUTE_CATEGORY = "/csap/metrics/host/attributes" ;
	public static final String METRICS_DATA_CATEGORY = "/csap/metrics/host/data" ;

	public static final List<String> INDEXED_FIELDS = Arrays.asList( APPID, PROJECT, LIFE, CATEGORY ) ;

	final static JsonWriterSettings jsonWriter = JsonWriterSettings.builder( ).outputMode( JsonMode.RELAXED )
			.build( ) ;

	final static ObjectMapper jacksonMapper = new ObjectMapper( ) ;

	static public JsonNode transformToJackson ( AggregateIterable<Document> results )
		throws IOException {

		ArrayNode resultArray = jacksonMapper.createArrayNode( ) ;
		var resultIterator = results.iterator( ) ;

		while ( resultIterator.hasNext( ) ) {

			Document next = resultIterator.next( ) ;
			resultArray.add( jacksonMapper.readTree( next.toJson( jsonWriter ) ) ) ;

		}

		JsonNode hostArrayNode = resultArray ;
		return hostArrayNode ;

	}

}
