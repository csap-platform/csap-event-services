
# csap-event-analytics

## Provides

The analytics service provides historical data feeds for the management portals, as well as adoption reports.

 
Refer to:  [CSAP Event Service](https://github.com/csap-platform/csap-event-services),


## Configuration

The following environment variables are required:
```
{
	"mongoHosts": "$serviceRef:mongoDb",
	"mongoUser": "$lifeCycleRef:mongoUser",
	"mongoPassword": "$lifeCycleRef:mongoPassword",
	"dataServiceUser": "$lifeCycleRef:dataServiceUser",
	"dataServicePass": "$lifeCycleRef:dataServicePass"
}
```


### Desktop development:
- Some tests require provisioned systems, such as LDAP, git, etc.
	- **application-company.yml**  is a small subset useful for quickly getting started
- refer to application.yml and application-company.yml for complete set of variables

- dependencies defined using maven, so any IDE works  
- create csap folder in your home directory, copy and modify
	- csapSecurity.properties
	- application-company.yml
- add the following parameter to your IDE start command 
	- ```--spring.config.location=file:c:/Users/yourHomeDir/csap/```
- add the following parameter to your JVM properties
	- ```-DcsapTest=/Users/yourHomeDir/csap/```

### Unit tests
- add the following to your env: ```-DcsapTest="/Users/yourHomeDir/csap/"```


References: [Spring Boot Configuration](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html)