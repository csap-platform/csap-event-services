package org.csap.events ;

import java.util.ArrayList ;
import java.util.List ;

import org.csap.events.db.EventDataReader ;
import org.csap.helpers.CSAP ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.mongodb.client.DistinctIterable ;

public class EventMetaData {

	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	private List appIds = new ArrayList( ) ;
	private List categories = new ArrayList( ) ;
	private List hosts = new ArrayList( ) ;
	private List<String> lifecycles = new ArrayList( ) ;
	private List projects = new ArrayList( ) ;
	private List uiUsers = new ArrayList( ) ;

	public EventMetaData ( ) {

	}

	@Override
	public String toString ( ) {

		return "EventMetaData{" + "appIds=" + appIds + ", \ncategories=" + categories + ", \nhosts=" + hosts
				+ ", \nlifecycles=" + lifecycles + ", \nprojects=" + projects + ",\n uiUsers=" + uiUsers + '}' ;

	}

	public List getAppIds ( ) {

		return appIds ;

	}

	public void setAppIds ( DistinctIterable<String> appIdsIterable ) {

		try {

			appIdsIterable.iterator( ).forEachRemaining( appId -> appIds.add( appId ) ) ;

		} catch ( Exception e ) {

			CsapMicroMeter.Utilities.supportForNonSpringConsumers( ).incrementCounter( EventDataReader.SEARCH_FILTER_KEY
					+ "errors" ) ;
			logger.error( "Failed to load: {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	public void setAppIds ( List appIds ) {

		this.appIds = appIds ;

	}

	public List getCategories ( ) {

		return categories ;

	}

	public void setCategories ( DistinctIterable<String> categoriesIterable ) {

		try {

			categoriesIterable.iterator( ).forEachRemaining( category -> categories.add( category ) ) ;

		} catch ( Exception e ) {

			CsapMicroMeter.Utilities.supportForNonSpringConsumers( ).incrementCounter( EventDataReader.SEARCH_FILTER_KEY
					+ "errors" ) ;
			logger.error( "Failed to load: {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	public void setCategories ( List categories ) {

		this.categories = categories ;

	}

	public List getProjects ( ) {

		return projects ;

	}

	public void setProjects ( DistinctIterable<String> projectsIterable ) {

		try {

			projectsIterable.iterator( ).forEachRemaining( project -> projects.add( project ) ) ;

		} catch ( Exception e ) {

			CsapMicroMeter.Utilities.supportForNonSpringConsumers( ).incrementCounter( EventDataReader.SEARCH_FILTER_KEY
					+ "errors" ) ;
			logger.error( "Failed to load: {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	public void setProjects ( List projects ) {

		this.projects = projects ;

	}

	public List getLifecycles ( ) {

		return lifecycles ;

	}

	public void setLifecycles ( DistinctIterable<String> lifecyclesIterable ) {

		try {

			lifecyclesIterable.iterator( ).forEachRemaining( life -> lifecycles.add( life ) ) ;

		} catch ( Exception e ) {

			CsapMicroMeter.Utilities.supportForNonSpringConsumers( ).incrementCounter( EventDataReader.SEARCH_FILTER_KEY
					+ "errors" ) ;
			logger.error( "Failed to load: {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	public void setLifecycles ( List lifecycles ) {

		this.lifecycles = lifecycles ;

	}

	public List getUiUsers ( ) {

		return uiUsers ;

	}

	public void setUiUsers ( DistinctIterable<String> uiUsersIterable ) {

		try {

			if ( uiUsersIterable == null ) {

				uiUsers.add( "NoMatchFound" ) ;

			} else {

				uiUsersIterable.iterator( ).forEachRemaining( uiUser -> {

					logger.debug( "user adding: {}", uiUser ) ;
					uiUsers.add( uiUser ) ;

				} ) ;

			}

		} catch ( Exception e ) {

			CsapMicroMeter.Utilities.supportForNonSpringConsumers( ).incrementCounter( EventDataReader.SEARCH_FILTER_KEY
					+ "errors" ) ;
			logger.error( "Failed to load: {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	public void setUiUsers ( List uiUsers ) {

		this.uiUsers = uiUsers ;

	}

	public List getHosts ( ) {

		return hosts ;

	}

	public void setHosts ( DistinctIterable<String> hostsIterable ) {

		try {

			hostsIterable.iterator( ).forEachRemaining( host -> hosts.add( host ) ) ;

		} catch ( Exception e ) {

			CsapMicroMeter.Utilities.supportForNonSpringConsumers( ).incrementCounter( EventDataReader.SEARCH_FILTER_KEY
					+ "errors" ) ;
			logger.error( "Failed to load: {}", CSAP.buildCsapStack( e ) ) ;

		}

	}

	public void setHosts ( List hosts ) {

		this.hosts = hosts ;

	}

}
