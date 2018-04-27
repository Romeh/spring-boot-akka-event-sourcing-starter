package com.spring.akka.eventsourcing.persistance.eventsourcing.annotations;


import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * a generic annotation to mark custom actor class as a managed spring bean
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public @interface PersistentActor {
}
