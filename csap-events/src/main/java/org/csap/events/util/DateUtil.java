package org.csap.events.util ;

import java.text.ParseException ;
import java.text.SimpleDateFormat ;
import java.util.Calendar ;
import java.util.Date ;
import java.util.TimeZone ;

import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class DateUtil {
	private static Logger logger = LoggerFactory.getLogger( DateUtil.class ) ;

	public static Date convertUserDateToJavaDate ( String userInterfaceDate ) {

		logger.debug( "date String {}", userInterfaceDate ) ;
		Date date = null ;

		try {

			SimpleDateFormat sdf = new SimpleDateFormat( "MM/dd/yyyy" ) ;
			sdf.setTimeZone( TimeZone.getTimeZone( "CST" ) ) ;
			date = sdf.parse( userInterfaceDate ) ;

		} catch ( ParseException e ) {

			logger.error( "Exception while parsing date", e ) ;

		}

		logger.debug( "Date: {}", date.toString( ) ) ;
		return date ;

	}

	public static String convertUserDateToMongoCreatedDate ( String userInterfaceDate ) {

		return convertJavaDateToMongoCreatedDate( convertUserDateToJavaDate( userInterfaceDate ) ) ;

	}

	public static String convertJavaCalendarToMongoCreatedDate ( Calendar calendar ) {

		String formatedDate = new SimpleDateFormat( "yyyy-MM-dd" ).format( calendar.getTime( ) ) ;
		return formatedDate ;

	}

	public static String convertJavaDateToMongoCreatedDate ( Date date ) {

		String formatedDate = new SimpleDateFormat( "yyyy-MM-dd" ).format( date.getTime( ) ) ;
		return formatedDate ;

	}

	public static String getFormatedDate ( Calendar calendar ) {

		String formatedDate = new SimpleDateFormat( "yyyy-MM-dd" ).format( calendar.getTime( ) ) ;
		return formatedDate ;

	}

	public static String getFormatedTime ( Calendar calendar ) {

		String formatedTime = new SimpleDateFormat( "HH:mm:ss" ).format( calendar.getTime( ) ) ;
		return formatedTime ;

	}

	public static Date getOffSetDate ( int offSet ) {

		Date date = null ;
		return date ;

	}

	public static String buildMongoCreatedDateFromOffset ( int offSet ) {

		Calendar dateToUpdateWithOffset = Calendar.getInstance( ) ;
		logger.debug( "Now: {} ",
				( new SimpleDateFormat( "yyyy-MM-dd" ).format( dateToUpdateWithOffset.getTime( ) ) ).toString( ) ) ;

		dateToUpdateWithOffset.add( Calendar.DAY_OF_YEAR, -( offSet ) ) ;
		String formatedDate = new SimpleDateFormat( "yyyy-MM-dd" ).format( dateToUpdateWithOffset.getTime( ) ) ;

		logger.debug( "Update with offset {} to be: {} ", Integer.toString( offSet ), formatedDate ) ;
		return formatedDate ;

	}

	public static Date getExpirationDate ( int offSet ) {

		Date date = null ;
		Calendar calendar = Calendar.getInstance( TimeZone.getTimeZone( "CST" ) ) ;
		calendar.set( Calendar.HOUR_OF_DAY, 0 ) ;
		calendar.set( Calendar.MINUTE, 0 ) ;
		calendar.set( Calendar.SECOND, 0 ) ;
		calendar.set( Calendar.MILLISECOND, 0 ) ;
		calendar.add( Calendar.MONTH, offSet ) ;
		date = calendar.getTime( ) ;
		return date ;

	}

	public static Calendar getDateFromString ( String date ) {

		Calendar calendar = Calendar.getInstance( ) ;

		if ( null != date ) {

			String[] dateArr = date.split( "-" ) ;

			if ( dateArr.length == 3 ) {

				calendar.set( Calendar.YEAR, Integer.parseInt( dateArr[0] ) ) ;
				calendar.set( Calendar.MONTH, ( Integer.parseInt( dateArr[1] ) - 1 ) ) ;
				calendar.set( Calendar.DAY_OF_MONTH, Integer.parseInt( dateArr[2] ) ) ;
				calendar.set( Calendar.HOUR_OF_DAY, 0 ) ;
				calendar.set( Calendar.MINUTE, 0 ) ;
				calendar.set( Calendar.SECOND, 0 ) ;
				calendar.set( Calendar.MILLISECOND, 0 ) ;

			}

		}

		return calendar ;

	}

	public static Calendar getDateWithOffSet ( int offSet ) {

		Calendar calendar = Calendar.getInstance( ) ;
		calendar.add( Calendar.DAY_OF_YEAR, -( offSet ) ) ;
		calendar.set( Calendar.HOUR_OF_DAY, 0 ) ;
		calendar.set( Calendar.MINUTE, 0 ) ;
		calendar.set( Calendar.SECOND, 0 ) ;
		calendar.set( Calendar.MILLISECOND, 0 ) ;
		return calendar ;

	}

	public static String getAnalyticsStartDate ( ) {

		return "2014-01-01" ;

	}

}
