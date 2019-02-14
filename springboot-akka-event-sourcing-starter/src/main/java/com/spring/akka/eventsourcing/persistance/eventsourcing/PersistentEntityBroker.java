package com.spring.akka.eventsourcing.persistance.eventsourcing;

import static akka.actor.Props.create;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.spring.akka.eventsourcing.config.PersistentEntityProperties;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

/**
 * generic spring managed bean persistent entity broker that abstract cluster sharding
 * for the enabled entity managed spring beans persistent actors
 */
@Service
public class PersistentEntityBroker {

	private final ActorSystem actorSystem;

	private final List<PersistentEntityProperties> persistentEntityProperties;

	private Map<Class<? extends PersistentEntity>, ActorRef> shardingRegistery;


	@Autowired
	public PersistentEntityBroker(ActorSystem actorSystem, List<PersistentEntityProperties> persistentEntityProperties) {
		this.actorSystem = actorSystem;
		this.persistentEntityProperties = persistentEntityProperties;
	}

	/**
	 * auto discover the different configuration of the different persistent entity actors and create a lookup of them for the broker
	 */
	@PostConstruct
	public void init() {

		Map<Class<? extends PersistentEntity>, ActorRef> initMap = new HashMap<>();

		persistentEntityProperties.forEach(persistentEntityConfig -> initMap.put(persistentEntityConfig.getEntityClass(),
				PersistentEntitySharding.of(create(persistentEntityConfig.getEntityClass(), persistentEntityConfig),
						persistentEntityConfig.persistenceIdPrefix(), persistentEntityConfig.persistenceIdPostfix(),
						persistentEntityConfig.numberOfShards()).shardRegion(actorSystem)));

		shardingRegistery = Collections.unmodifiableMap(initMap);

	}

	/**
	 * @param entityClass the persistent entity class to get the cluster sharding for
	 * @return the found actor ref based into the sharding registery lookup
	 */
	public ActorRef findPersistentEntity(Class<? extends PersistentEntity> entityClass) {

		final ActorRef sharedRegionPersistentActor = shardingRegistery.get(entityClass);
		if (sharedRegionPersistentActor == null) {
			throw new IllegalStateException("no persistent entity configuration(PersistentEntityProperties) has been define for that entity class " + entityClass.getSimpleName());
		}
		return sharedRegionPersistentActor;


	}


}
