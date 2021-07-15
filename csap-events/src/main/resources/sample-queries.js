


db.metrics.find({ "attributes.hostName": "csap-dev01", "attributes.id": "service_30", 
    "createdOn.lastUpdatedOn":{ $gte: ISODate("2019-12-23")} }).limit( 5 ) ;

// find 5 items

db.metrics.find({ "attributes.hostName": "csap-dev06", "attributes.id": "service_3600", 
    "createdOn.lastUpdatedOn":{ $gte: ISODate("2019-12-16"), $lte: ISODate("2019-12-18T00:00:00.000-0500") } }).limit( 5 ) ;

// count them
db.metrics.find({ "attributes.hostName": "csap-dev06", "attributes.id": "service_3600", 
    "createdOn.lastUpdatedOn":{ $gte: ISODate("2019-12-16"), $lte: ISODate("2019-12-18T00:00:00.000-0500") } }).count() ;



db.metrics.find({ "attributes.hostName": "csap-dev06", "attributes.id": "service_3600", 
    "createdOn.lastUpdatedOn":{ $gte: ISODate("2019-12-16T17:06:15.601-0500"), $lte: ISODate("2019-12-18T17:06:15.601-0500") } }) ;

//{attributes.hostName=csap-dev06, attributes.id=service_3600, createdOn.lastUpdatedOn={$gte=2019-12-16T17:06:15.601-0500, $lte=2019-12-18T17:06:15.601-0500}} 



//db.metrics.count();

//from now: attributes.hostName=csap-dev06, attributes.id=service_3600, createdOn.lastUpdatedOn={$gte=2019-12-16T17:10:49.091-0500, $lte=2019-12-18T17:10:49.091-0500
db.metrics.find({ "attributes.hostName": "csap-dev06", "attributes.id": "service_3600", 
 "createdOn.lastUpdatedOn":{ $gte: ISODate("2019-12-16"), $lte: ISODate("2019-12-18T00:00:00.000-0500") } });

//from 1 day ago:  attributes.hostName=csap-dev06, attributes.id=service_3600, createdOn.lastUpdatedOn={$gte=2019-12-16T00:00:00.000-0500, $lte=2019-12-18T00:00:00.000-0500}
db.metrics.find({ "attributes.hostName": "csap-dev06", "attributes.id": "service_3600", 
 "createdOn.lastUpdatedOn":{ $gte: ISODate("2019-12-16T17:06:15.601-0500"), $lte: ISODate("2019-12-18T17:06:15.601-0500") } }) ;
 


//
//  Events: find trend example
//
db.eventRecords.find( {
    "appId" : "SensusCsap",
    "project" : "CSAP Platform",
    "lifecycle" : "dev",
    "category" : "/csap/reports/os-process/daily",
    "createdOn.date" : {
      "$gt" : "2021-02-09",
      "$lte" : "2021-02-12"
      },
    "data.summary" : {
      "$elemMatch" : {
        "$or" : [ {
          "serviceName" :
        "test-k8s-csap-reference"
        } ]
      }
    }
})


//
//  Trend aggregation pipleine
//
db.eventRecords.aggregate( [
    {  
        "$match": {
            "appId" : "SensusCsap",
            "project" : "CSAP Platform",
            "lifecycle" : "dev",
            "category" : "/csap/reports/os-process/daily",
            "createdOn.date" : {
            "$gte" : "2021-02-10",
              "$lte" : "2021-02-12"
              },
            "data.summary" : {
              "$elemMatch" : {
                "$or" : [ {
                  "serviceName" :
                "test-k8s-csap-reference"
                } ]
              }
            }
        }
    },
    {
    "$unwind" : "$data.summary"
    },
    {  
        "$match": {
            "$or" : [ {
                "data.summary.serviceName" : "test-k8s-csap-reference"
            } ]
        }
    },
    {
        "$group" : {
            "_id" : {
              "lifecycle" : "$lifecycle",
              "date" : "$createdOn.date",
              "appId" : "$appId",
              "host" : "$host",
              "project" :
                "$project",
              "serviceName" : "$data.summary.serviceName"
            },
            "threadCount" : {
              "$sum" : "$data.summary.threadCount"
            },
            "numberOfSamples" : {
                "$sum" : "$data.summary.numberOfSamples"
            },
            "countCsapMean" : {
                "$sum" : "$data.summary.countCsapMean"
            }
        }
    },
    {
      "$project" : {
            "appId" : 1,
            "lifecycle" : 1,
            "project" : 1,
            "serviceName" : 1,
            "date" : 1,
            "threadCount" : 1,
            "numberOfSamples" : {
             
                "$cond" : [ {
                "$eq" : [ "$numberOfSamples", 0 ]
                }, 1, "$numberOfSamples" ]
            },
            "countCsapMean" : {
             
                "$cond" : [ 
                    {
                        "$eq" : [ "$countCsapMean", 0 ]
                    },
                    1,
                    "$countCsapMean" 
                ]
            }
        }
    },
    {
      "$project" : {
            "appId" : 1,
            "project" : 1,
            "lifecycle" : 1,
            "host" : 1,
            "numberOfSamples" : 1,
            "date" : 1,
            "threadCount" : {
                "$divide" : [ "$threadCount", "$numberOfSamples" ]
            },
            "countCsapMean" : 1,
            "threadCountTotal" : {
                "$divide" : [ 
                    { "$multiply" : [ "$threadCount", "$countCsapMean" ] }, 
                    "$numberOfSamples" 
                ]
            }
      }
    }
] )














//
// Working
//
p = [



	
	

	{
  "$group" : {
    "_id" : {
      "lifecycle" : "$_id.lifecycle",
      "date" : "$_id.date",
      "appId" : "$_id.appId",
      "project" : "$_id.project",
     
		"serviceName" : "$_id.serviceName"
    },
    "threadCount" : {
      "$sum" : "$threadCount"
    },
    "numberOfSamples" : {
      "$sum" : "$numberOfSamples"
    }
 
		}
	},
		{
	  "$project" : {
	    "threadCount" : 1,
	    "appId" : "$_id.appId",
	    "lifecycle" : "$_id.lifecycle",
	    "project" : "$_id.project",
	    "serviceName" :
			"$_id.serviceName",
	    "date" : "$_id.date",
	    "_id" : 0,
	    "numberOfSamples" : 1
	  }
	},
		{
	  "$project" : {
	    "appId" : 1,
	    "lifecycle" : 1,
	    "project" : 1,
	    "serviceName" : 1,
	    "date" : 1,
	    "threadCount" : 1,
	    "numberOfSamples" : {
	     
			"$cond" : [ {
	        "$eq" : [ "$numberOfSamples", 0 ]
	      }, 1, "$numberOfSamples" ]
	    }
	  }
	},
		{
	  "$sort" : {
	    "date" : 1
	  }
	},
		{
	  "$group" : {
	    "_id" : {
	      "lifecycle" : "$lifecycle",
	      "appId" : "$appId",
	      "project" : "$project",
	      "serviceName" : "$serviceName"
	    },
	   
			"date" : {
	      "$push" : "$date"
	    },
	    "threadCount" : {
	      "$push" : "$threadCount"
	    }
	  }
	},
		{
	  "$project" : {
	    "threadCount" : 1,
	    "serviceName" : "$_id.serviceName",
	    "appId" : "$_id.appId",
	    "lifecycle" : "$_id.lifecycle",
	    "project" :
			"$_id.project",
	    "date" : 1,
	    "_id" : 0
	  }
	} 
]