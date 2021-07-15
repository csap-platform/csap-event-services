package org.csap.test.container ;

import java.lang.annotation.ElementType ;
import java.lang.annotation.Retention ;
import java.lang.annotation.RetentionPolicy ;
import java.lang.annotation.Target ;

import org.csap.events.CsapEventsApplication ;
import org.csap.test.reports.TestSpringConfiguration ;
import org.junit.jupiter.api.TestInstance ;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc ;
import org.springframework.boot.test.context.SpringBootTest ;
import org.springframework.test.context.ActiveProfiles ;

public class CsapEventsTests {

	@Target ( ElementType.TYPE )
	@Retention ( RetentionPolicy.RUNTIME )
	@SpringBootTest ( classes = {
			TestSpringConfiguration.class, CsapEventsApplication.class,
	} )

	@AutoConfigureMockMvc
	@ActiveProfiles ( "junit" )
	@TestInstance ( TestInstance.Lifecycle.PER_CLASS )
	public @interface MockTests {
	}

}
