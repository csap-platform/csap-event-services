package org.csap.test.container ;

import static org.assertj.core.api.Assertions.assertThat ;

import org.csap.events.CsapEventsApplication ;
import org.csap.events.LandingPage ;
import org.csap.helpers.CsapApplication ;
import org.junit.jupiter.api.Test ;
import org.junit.jupiter.api.TestInstance ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;
import org.springframework.beans.factory.annotation.Autowired ;
import org.springframework.boot.test.context.SpringBootTest ;
import org.springframework.context.ApplicationContext ;
import org.springframework.test.context.ActiveProfiles ;

@SpringBootTest ( classes = {
		CsapEventsApplication.class
} )
@ActiveProfiles ( "junit" )
@TestInstance ( TestInstance.Lifecycle.PER_CLASS )

class Boot_Container {
	Logger logger = LoggerFactory.getLogger( Boot_Container.class ) ;

	static {

		CsapApplication.initialize( "" ) ;

	}

	@Autowired
	ApplicationContext applicationContext ;

	@Test
	void load_context ( ) {

		logger.info( CsapApplication.testHeader( ) ) ;

		assertThat( applicationContext.getBeanDefinitionCount( ) )
				.as( "Spring Bean count" )
				.isGreaterThan( 100 ) ;

		assertThat( applicationContext.getBean( LandingPage.class ) )
				.as( "SpringRequests controller loaded" )
				.isNotNull( ) ;

	}

}
