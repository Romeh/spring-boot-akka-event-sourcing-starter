package com.spring.akka.eventsourcing.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties(prefix = "spring.akka")
@Getter
@Setter
public class AkkaProperties {

	private String systemName;
	private Config config;

	public void setConfig(String config) {
		Config defaultConfig = ConfigFactory.empty();
		this.config = defaultConfig.withFallback(ConfigFactory.load((config))).resolve();
	}


}
