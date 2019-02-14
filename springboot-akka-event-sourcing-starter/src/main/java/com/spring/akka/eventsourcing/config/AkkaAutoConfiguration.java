package com.spring.akka.eventsourcing.config;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.spring.akka.eventsourcing.persistance.eventsourcing.PersistentEntity;
import com.spring.akka.eventsourcing.persistance.eventsourcing.PersistentEntityBroker;

import akka.actor.ActorSystem;

@Configuration
@ConditionalOnClass(PersistentEntity.class)
@EnableConfigurationProperties(AkkaProperties.class)
public class AkkaAutoConfiguration {

	@Bean(destroyMethod = "terminate")
	@ConditionalOnMissingBean
	public ActorSystem getActorSystem(AkkaProperties akkaProperties) {
		ActorSystem system;
		if (akkaProperties.getSystemName() != null && akkaProperties.getConfig() != null) {
			system = ActorSystem.create(akkaProperties.getSystemName(), akkaProperties.getConfig());
		} else if (akkaProperties.getSystemName() != null) {
			system = ActorSystem.create(akkaProperties.getSystemName());
		} else {
			system = ActorSystem.create();
		}
		return system;
	}

	@Bean
	@ConditionalOnMissingBean
	public PersistentEntityBroker persistentEntityBroker(ActorSystem actorSystem, List<PersistentEntityProperties> persistentEntityProperties) {
		return new PersistentEntityBroker(actorSystem, persistentEntityProperties);
	}

}
