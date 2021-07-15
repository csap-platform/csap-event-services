package org.csap.events.db ;

import static org.csap.events.util.MongoConstants.ANALYTICS_DB_NAME ;
import static org.csap.events.util.MongoConstants.METRICS_ATTRIBUTES_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.METRICS_DATA_COLLECTION_NAME ;
import static org.csap.events.util.MongoConstants.METRICS_DB_NAME ;
import static org.csap.events.util.MongoConstants.TOP_SERVICE_COLLECTION ;

import java.util.Calendar ;

import javax.inject.Inject ;

import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.mongodb.BasicDBObject ;
import com.mongodb.DBCollection ;
import com.mongodb.MongoClient ;
import com.mongodb.WriteConcern ;
import com.mongodb.WriteResult ;

public class ServiceAnalytics {
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Inject
	private MongoClient mongoClient ;

	public void isScheduleJobRunning ( ) {

		DBCollection collection = mongoClient.getDB( ANALYTICS_DB_NAME ).getCollection( "scheduledjobstatus" ) ;

		BasicDBObject query = new BasicDBObject( "_id", "job" ) ;
		query.append( "running", false ) ;
		Calendar calendar = Calendar.getInstance( ) ;
		// calendar.

		BasicDBObject updateObj = new BasicDBObject( "_id", "job" ) ;
		updateObj.append( "running", true ) ;
		updateObj.append( "lastUpdated", Calendar.getInstance( ).getTime( ) ) ;

		BasicDBObject setObject = new BasicDBObject( ) ;
		setObject.put( "$set", updateObj ) ;

		WriteResult result = collection.update( query, setObject, false, false, WriteConcern.ACKNOWLEDGED ) ;

		logger.debug( "Result::-->>>" + result ) ;
		logger.debug( "Result::" + result.getN( ) ) ;

	}

	/*
	 * private Set<String> getCpuServiceName(String id,String hostName){
	 * 
	 * BasicDBObject query = new BasicDBObject(); query.append(HOST_NAME,hostName);
	 * query.append(ID, id); BasicDBObject sortOrder = new BasicDBObject();
	 * sortOrder.append(CREATED_ON +"."+ LAST_UPDATED_ON, -1); DBCollection
	 * dbCollection = getMetricsAttributeCollection(); BasicDBObject keys = new
	 * BasicDBObject(); keys.put("_id",0); keys.put("graphs.Cpu_15s", 1); DBCursor
	 * cursor = dbCollection.find(query,keys).sort(sortOrder); Set<String>
	 * serviceNames = null; if(cursor.hasNext()){ BasicDBObject cpuServiceObject =
	 * (BasicDBObject) cursor.next(); if(null != cpuServiceObject){ DBObject
	 * graphObject = (DBObject) cpuServiceObject.get("graphs"); if(null !=
	 * graphObject){ DBObject cpu15s = (DBObject) graphObject.get("Cpu_15s");
	 * if(null != cpu15s){ serviceNames = cpu15s.keySet(); } } } }
	 * 
	 * 
	 * return serviceNames; }
	 */
	/*
	 * public List<String> getTopServiceNames(String id,String host){ DBCollection
	 * serviceDbCollection = getAnalyticsCollection();
	 * 
	 * BasicDBObject idAndHost = new BasicDBObject(ID, id);
	 * idAndHost.append(HOST_NAME, host); BasicDBObject query = new
	 * BasicDBObject("_id",idAndHost); BasicDBObject match = new
	 * BasicDBObject("$match", query);
	 * 
	 * DBObject unwind = new BasicDBObject( "$unwind", "$topCpuService");
	 * 
	 * BasicDBObject sortOrder = new BasicDBObject("topCpuService.cpu", -1);
	 * BasicDBObject sort = new BasicDBObject("$sort", sortOrder);
	 * 
	 * List<DBObject> operations = new ArrayList<>(); operations.add(match);
	 * operations.add(unwind); operations.add(sort);
	 * 
	 * AggregationOutput output = serviceDbCollection.aggregate(operations);
	 * List<String> topServiceList = new ArrayList<>(); if(null != output && null !=
	 * output.results()){ Iterator<DBObject> topServiceIterator =
	 * output.results().iterator(); int i = 0; while(topServiceIterator.hasNext() &&
	 * i < 5){ DBObject serviceObject = topServiceIterator.next(); String
	 * serviceName =
	 * (String)((DBObject)serviceObject.get("topCpuService")).get("serviceName") ;
	 * if(!"totalCpu".equalsIgnoreCase(serviceName)){
	 * topServiceList.add(serviceName); i++; } } } return topServiceList; }
	 */
	/*
	 * public void updateDBwithTopServices(){ logger.info("Updating top services");
	 * Split split = SimonManager.getStopwatch("updateDbWithTopService").start();
	 * Iterator<DBObject> serviceAndHost = getUniqueServiceHostsForCurrentDay();
	 * DBCollection serviceDbCollection = getAnalyticsCollection();
	 * while(serviceAndHost.hasNext()){ DBObject serviceHostObj =
	 * serviceAndHost.next(); DBObject idObject = (DBObject)
	 * serviceHostObj.get("_id"); String id = (String) idObject.get(ID); String host
	 * = (String) idObject.get(HOST_NAME); //logger.debug("ID::{} Host:{}",id,host);
	 * if(null == id || null == host) continue; Set<String> cpuServiceNames =
	 * getCpuServiceName(id, host); if(null == cpuServiceNames) continue;
	 * BasicDBList topServiceList = new BasicDBList(); for(String serviceName :
	 * cpuServiceNames){ DBObject cpuTotal = getServiceCpuTotal(id, host,
	 * serviceName); Object cpu = cpuTotal.get(serviceName); BasicDBObject dbObject
	 * = new BasicDBObject("serviceName",serviceName); dbObject.append("cpu",cpu);
	 * topServiceList.add(dbObject); } BasicDBObject idAndHost = new
	 * BasicDBObject(ID, id); idAndHost.append(HOST_NAME, host); BasicDBObject query
	 * = new BasicDBObject("_id",idAndHost);
	 * 
	 * BasicDBObject cpuPerIdPerHost = new BasicDBObject("_id",idAndHost);
	 * cpuPerIdPerHost.append("topCpuService", topServiceList);
	 * 
	 * BasicDBObject setObject = new BasicDBObject(); setObject.put("$set",
	 * cpuPerIdPerHost); serviceDbCollection.update(query, setObject,true,false); }
	 * split.stop(); }
	 */
	/*
	 * private DBObject getServiceCpuTotal(String id,String hostName,String
	 * serviceName){ BasicDBObject query = constructQuery(hostName,id,1,0);
	 * BasicDBObject match = new BasicDBObject("$match", query);
	 * 
	 * DBObject fields = new BasicDBObject("data."+serviceName, 1);
	 * fields.put("attributes.id", 1); fields.put("attributes.hostName", 1);
	 * DBObject project = new BasicDBObject("$project", fields);
	 * 
	 * DBObject unwind = new BasicDBObject( "$unwind", "$data."+serviceName);
	 * 
	 * Map<String, Object> groupFieldMap = new HashMap<>();
	 * groupFieldMap.put(HOST_NAME,"$"+ATTRIBUTES+ "." +HOST_NAME);
	 * groupFieldMap.put(ID,"$"+ATTRIBUTES + "." + ID); DBObject groupFields = new
	 * BasicDBObject("_id",new BasicDBObject(groupFieldMap));
	 * groupFields.put(serviceName, new BasicDBObject( "$sum",
	 * "$data."+serviceName)); DBObject group = new BasicDBObject("$group",
	 * groupFields);
	 * 
	 * List<DBObject> operations = new ArrayList<>(); operations.add(match);
	 * operations.add(project); operations.add(unwind); operations.add(group);
	 * 
	 * DBCollection dbCollection = getMetricsDataCollection(); AggregationOutput
	 * output = dbCollection.aggregate(operations); DBObject result = null; if(null
	 * != output && null != output.results() &&
	 * output.results().iterator().hasNext()){ result =
	 * output.results().iterator().next(); } return result;
	 * 
	 * }
	 */
	/*
	 * private Iterator<DBObject> getUniqueServiceHostsForCurrentDay(){ List<String>
	 * serviceResourceIds = getServiceResourceIds(); Split split =
	 * SimonManager.getStopwatch("uniqueServiceHost").start(); BasicDBList
	 * resourceList = new BasicDBList(); resourceList.addAll(serviceResourceIds);
	 * DBObject matchClause = new BasicDBObject("$in", resourceList); String today =
	 * DateUtil.getFormatedDate(Calendar.getInstance()); DBObject query = new
	 * BasicDBObject(); query.put(CREATED_ON + "."+ DATE,today);
	 * query.put(ATTRIBUTES + "." + ID, matchClause); BasicDBObject match = new
	 * BasicDBObject("$match", query);
	 * 
	 * Map<String, Object> groupFieldMap = new HashMap<>();
	 * groupFieldMap.put(HOST_NAME,"$"+ATTRIBUTES+ "." +HOST_NAME);
	 * groupFieldMap.put(ID,"$"+ATTRIBUTES + "." + ID); DBObject groupFields = new
	 * BasicDBObject("_id",new BasicDBObject(groupFieldMap)); DBObject group = new
	 * BasicDBObject("$group", groupFields);
	 * 
	 * List<DBObject> aggregationPipeline = new ArrayList<>();
	 * aggregationPipeline.add(match); aggregationPipeline.add(group);
	 * 
	 * DBCollection dbCollection = getMetricsDataCollection(); AggregationOutput
	 * output = dbCollection.aggregate(aggregationPipeline); split.stop(); return
	 * output.results().iterator(); }
	 */
	/*
	 * private List<String> getServiceResourceIds(){ Split split =
	 * SimonManager.getStopwatch("serviceResourceIds").start(); List<String>
	 * servicResourceIds = new ArrayList<>(); DBCollection dbCollection =
	 * getMetricsDataCollection(); String today =
	 * DateUtil.getFormatedDate(Calendar.getInstance()); DBObject query = new
	 * BasicDBObject(CREATED_ON + "."+ DATE,today); List idList =
	 * dbCollection.distinct(ATTRIBUTES + "." + ID,query); if(null != idList){
	 * for(Object obj : idList){ String id = (String) obj;
	 * if(id.startsWith("service")){ servicResourceIds.add(id); } } } split.stop();
	 * return servicResourceIds; }
	 */
	/*
	 * private BasicDBObject constructQuery(String hostName,String id,int
	 * numDays,int dateOffSet){ BasicDBObject query = new BasicDBObject();
	 * query.append(ATTRIBUTES+ "." +HOST_NAME,hostName); query.append(ATTRIBUTES +
	 * "." + ID, id); int fromDateOffSet = numDays + dateOffSet; int toDateOffSet =
	 * dateOffSet; Calendar startTime = Calendar.getInstance();
	 * startTime.add(Calendar.DAY_OF_YEAR, -(fromDateOffSet)); Calendar endTime =
	 * Calendar.getInstance(); endTime.add(Calendar.DAY_OF_YEAR, -(toDateOffSet));
	 * query.append(CREATED_ON+"."+LAST_UPDATED_ON, new
	 * BasicDBObject("$gte",startTime.getTime()).append("$lte", endTime.getTime()));
	 * return query; }
	 */
	private DBCollection getAnalyticsCollection ( ) {

		return mongoClient.getDB( ANALYTICS_DB_NAME ).getCollection( TOP_SERVICE_COLLECTION ) ;

	}

	private DBCollection getMetricsDataCollection ( ) {

		return mongoClient.getDB( METRICS_DB_NAME ).getCollection( METRICS_DATA_COLLECTION_NAME ) ;

	}

	private DBCollection getMetricsAttributeCollection ( ) {

		return mongoClient.getDB( METRICS_DB_NAME ).getCollection( METRICS_ATTRIBUTES_COLLECTION_NAME ) ;

	}

}
