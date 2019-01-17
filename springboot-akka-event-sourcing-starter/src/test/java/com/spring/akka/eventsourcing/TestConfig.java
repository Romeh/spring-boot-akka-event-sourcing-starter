package com.spring.akka.eventsourcing;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.spring.akka.eventsourcing.config.AkkaProperties;

/**
 * @author romeh
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan("com.spring.akka.eventsourcing")
public class TestConfig {
	@Bean
	public AkkaProperties akkaProperties() {
		AkkaProperties akkaProperties = new AkkaProperties();
		akkaProperties.setConfig("akka.initializer.conf");
		akkaProperties.setSystemName("testActorSystem");
		return akkaProperties;
	}
}

