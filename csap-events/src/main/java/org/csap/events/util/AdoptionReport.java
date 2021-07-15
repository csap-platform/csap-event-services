package org.csap.events.util ;

import java.util.concurrent.TimeUnit ;

import javax.inject.Inject ;

import org.csap.events.db.CsapAdoptionReportBuilder ;
import org.csap.helpers.CSAP ;
import org.csap.integations.CsapMicroMeter ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.scheduling.annotation.Async ;
import org.springframework.scheduling.annotation.Scheduled ;
import org.springframework.stereotype.Service ;

@Service
public class AdoptionReport {
	private Logger logger = LoggerFactory.getLogger( getClass( ) ) ;

	@Inject
	private CsapAdoptionReportBuilder adoptionReportBuilder ;
	@Autowired
	CsapMicroMeter.Utilities metricUtilities ;

	// Every night 15 minutes after midnight
	@Async
	@Scheduled ( cron = "0 15 0 * * ?" )
	public void dailySchedulerForAdoptionReport ( ) {

		var timer = metricUtilities.startTimer( ) ;

		logger.info( "Daily Adoption Report - Starting" ) ;
		adoptionReportBuilder.buildAdoptionReportAndSaveToDB( 1 ) ;

		var nanos = metricUtilities.stopTimer( timer, "AdoptionReport" ) ;

		logger.info( "Daily Adoption Report - Completed, Time Taken: {}",
				CSAP.timeUnitPresent( TimeUnit.NANOSECONDS.toMillis( nanos ) ) ) ;

	}

}
