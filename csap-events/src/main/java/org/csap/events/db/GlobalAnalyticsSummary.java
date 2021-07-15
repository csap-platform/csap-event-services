package org.csap.events.db ;

import org.bson.Document ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class GlobalAnalyticsSummary {

	private String projectName ;
	private String deploymentName ;
	private String csapVersion ;
	private String appId ;
	private int vms ;
	private int serviceCount ;
	private int instanceCount ;
	private int cpuCount ;
	private double totalLoad ;
	private int numSamples ;
	private int activeUsers ;
	private int totalActivity ;
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	public GlobalAnalyticsSummary ( ) {

	}

	public GlobalAnalyticsSummary ( String projectName ) {

		this.projectName = projectName ;

	}

	public GlobalAnalyticsSummary ( Document dbObject ) {

		retrieveDataFromDbObject( dbObject ) ;

	}

	public String getProjectName ( ) {

		return projectName ;

	}

	public void setProjectName ( String projectName ) {

		this.projectName = projectName ;

	}

	public String getCsapVersion ( ) {

		return csapVersion ;

	}

	public void setCsapVersion ( String csapVersion , String life ) {

		if ( null == this.csapVersion ) {

			this.csapVersion = csapVersion ;

		} else {

			if ( "prod".equalsIgnoreCase( life ) ) {

				this.csapVersion = csapVersion ;

			}

		}

	}

	public int getVms ( ) {

		return vms ;

	}

	public void setVms ( Object vms ) {

		if ( null != vms )
			this.vms += (Integer) vms ;

	}

	public int getServiceCount ( ) {

		return serviceCount ;

	}

	public void setServiceCount ( Object serviceCount ) {

		if ( null != serviceCount )
			this.serviceCount += (Integer) serviceCount ;

	}

	public double getTotalLoad ( ) {

		return totalLoad ;

	}

	public void setTotalLoad ( Object totalLoad ) {

		if ( null != totalLoad )
			this.totalLoad += (Double) totalLoad ;

	}

	public int getNumSamples ( ) {

		return numSamples ;

	}

	public void setNumSamples ( Object numSamples ) {

		if ( null != numSamples )
			this.numSamples += (Integer) numSamples ;

	}

	public int getInstanceCount ( ) {

		return instanceCount ;

	}

	public void setInstanceCount ( Object instanceCount ) {

		if ( null != instanceCount )
			this.instanceCount += (Integer) instanceCount ;

	}

	public int getCpuCount ( ) {

		return cpuCount ;

	}

	public void setCpuCount ( Object cpuCount ) {

		if ( null != cpuCount )
			this.cpuCount += (Integer) cpuCount ;

	}

	public int getActiveUsers ( ) {

		return activeUsers ;

	}

	public void setActiveUsers ( int activeUsers ) {

		this.activeUsers += activeUsers ;

	}

	public int getTotalActivity ( ) {

		return totalActivity ;

	}

	public void setTotalActivity ( Object totalActivity ) {

		if ( null != totalActivity )
			this.totalActivity += (Integer) totalActivity ;

	}

	public String getDeploymentName ( ) {

		return deploymentName ;

	}

	public void setDeploymentName ( String deploymentName ) {

		this.deploymentName = deploymentName ;

	}

	public String getAppId ( ) {

		return appId ;

	}

	public void setAppId ( String appId ) {

		this.appId = appId ;

	}

	public void addVmSummary ( Object packObj ) {

		Document packageObject = (Document) packObj ;
		setVms( packageObject.get( "vms" ) ) ;
		setServiceCount( packageObject.get( "services" ) ) ;
		Document instanceObject = (Document) packageObject.get( "instances" ) ;

		if ( null != instanceObject ) {

			setInstanceCount( instanceObject.get( "total" ) ) ;

		} else {

			logger.error( "instance object is null for {} ", packObj.toString( ) ) ;

		}

	}

	public void add ( GlobalAnalyticsSummary summary ) {

		this.cpuCount += summary.getCpuCount( ) ;
		this.instanceCount += summary.getInstanceCount( ) ;
		this.numSamples += summary.getNumSamples( ) ;
		this.serviceCount += summary.getServiceCount( ) ;
		this.totalActivity += summary.getTotalActivity( ) ;
		this.totalLoad += summary.getTotalLoad( ) ;
		this.vms += summary.getVms( ) ;
		this.activeUsers += summary.getActiveUsers( ) ;

	}

	public void retrieveDataFromDbObject ( Document dbObject ) {

		this.projectName = (String) dbObject.get( "projectName" ) ;
		this.deploymentName = (String) dbObject.get( "deploymentName" ) ;
		this.csapVersion = (String) dbObject.get( "csapVersion" ) ;
		this.appId = (String) dbObject.get( "appId" ) ;
		this.vms = (Integer) dbObject.get( "vms" ) ;
		this.serviceCount = (Integer) dbObject.get( "serviceCount" ) ;
		this.instanceCount = (Integer) dbObject.get( "instanceCount" ) ;
		this.cpuCount = (Integer) dbObject.get( "cpuCount" ) ;
		this.totalLoad = (Double) dbObject.get( "totalLoad" ) ;
		this.numSamples = (Integer) dbObject.get( "numSamples" ) ;
		this.activeUsers = (Integer) dbObject.get( "activeUsers" ) ;
		this.totalActivity = (Integer) dbObject.get( "totalActivity" ) ;

	}

	@Override
	public String toString ( ) {

		return "GlobalAnalyticsSummary [projectName=" + projectName + ", deploymentName=" + deploymentName
				+ ", csapVersion=" + csapVersion + ", appId=" + appId + ", vms=" + vms + ", serviceCount="
				+ serviceCount + ", instanceCount=" + instanceCount + ", cpuCount=" + cpuCount + ", totalLoad="
				+ totalLoad + ", numSamples=" + numSamples + ", activeUsers=" + activeUsers + ", totalActivity="
				+ totalActivity + "]" ;

	}

}
