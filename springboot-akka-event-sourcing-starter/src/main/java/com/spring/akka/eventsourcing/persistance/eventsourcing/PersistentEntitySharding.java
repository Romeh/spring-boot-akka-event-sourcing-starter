package com.spring.akka.eventsourcing.persistance.eventsourcing;

import java.util.function.Function;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ClusterShardingSettings;
import akka.cluster.sharding.ShardRegion;
import akka.cluster.sharding.ShardRegion.MessageExtractor;
import akka.persistence.AbstractPersistentActor;

/**
 * Base class for setting up sharding of persistent actors that:
 * <p>
 * - Have their persistenceId conform to [prefix] + "_" + [id], e.g. "doc_249e1098-1cd9-4ffe-a494-73a430983590"
 * - Respond to a specific base class of commands
 * - Send the commands unchanged from the SharedRegion router onto the persistent actors themselves
 *
 * @param <C> Base class of commands that the actor will respond to
 */

public class PersistentEntitySharding<C> {
	private final Props props;
	private final String persistenceIdPrefix;
	private final Function<C, String> persistenceIdPostfix;
	private final int numberOfShards;
	private final MessageExtractor messageExtractor = new MessageExtractor() {
		@Override
		public String entityId(Object command) {
			return getEntityId(command);
		}

		@Override
		public String shardId(Object command) {
			if (command instanceof ShardRegion.StartEntity) {
				ShardRegion.StartEntity startEntity = (ShardRegion.StartEntity) command;
				return getShardId(startEntity.entityId());
			} else {
				return getShardId(getEntityId(command));
			}

		}

		@Override
		public Object entityMessage(Object command) {
			// we don't need need to unwrap messages sent to the router, they can just be
			// forwarded to the target persistent actor directly.
			return command;
		}
	};

	protected PersistentEntitySharding(Props props, String persistenceIdPrefix, Function<C, String> entityIdForCommand, int numberOfShards) {
		this.props = props;
		this.persistenceIdPrefix = persistenceIdPrefix;
		this.persistenceIdPostfix = entityIdForCommand;
		this.numberOfShards = numberOfShards;
	}

	/**
	 * Creates a PersistentEntitySharding for an actor that is created according to [props]. The actor must be a subclass of {@link AbstractPersistentActor}.
	 * Entities will be sharded onto 256 shards.
	 *
	 * @param persistenceIdPrefix  Fixed prefix for each persistence id. This is typically the name of your aggregate root, e.g. "document" or "user".
	 * @param persistenceIdPostfix Function that returns the last part of the persistence id that a command is routed to. This typically is the real ID of your entity, or UUID.
	 */
	public static <C> PersistentEntitySharding<C> of(Props props, String persistenceIdPrefix, Function<C, String> persistenceIdPostfix) {
		return new PersistentEntitySharding<>(props, persistenceIdPrefix, persistenceIdPostfix, 256);
	}

	/**
	 * Creates a PersistentEntitySharding for an actor that is created according to [props]. The actor must be a subclass of {@link AbstractPersistentActor}.
	 *
	 * @param numberOfShards       Number of shards to divide all entity/persistence ids into. This can not be changed after the first run.
	 * @param persistenceIdPrefix  Fixed prefix for each persistence id. This is typically the name of your aggregate root, e.g. "document" or "user".
	 * @param persistenceIdPostfix Function that returns the last part of the persistence id that a command is routed to. This typically is the real ID of your entity, or UUID.
	 */
	public static <C> PersistentEntitySharding<C> of(Props props, String persistenceIdPrefix, Function<C, String> persistenceIdPostfix, int numberOfShards) {
		return new PersistentEntitySharding<>(props, persistenceIdPrefix, persistenceIdPostfix, numberOfShards);
	}

	/**
	 * Starts the cluster router (ShardRegion) for this persistent actor type on the given actor system,
	 * and returns its ActorRef. If it's already running, just returns the ActorRef.
	 */
	public ActorRef shardRegion(ActorSystem system) {
		return ClusterSharding.get(system).start(
				persistenceIdPrefix,
				props,
				ClusterShardingSettings.create(system),
				messageExtractor);
	}

	/**
	 * Returns the postfix part of a generated persistence ID.
	 *
	 * @param persistenceId A persistenceId of an actor that was spawned by sending a command to it through the
	 *                      ActorRef returned by {@link #shardRegion}.
	 */
	public String getPersistenceIdPostfix(String persistenceId) {
		return persistenceId.substring(persistenceIdPrefix.length() + 1);
	}

	/**
	 * Returns the entityId (=persistenceId) to which the given command should be routed
	 */
	@SuppressWarnings("unchecked")
	public String getEntityId(Object command) {
		return persistenceIdPrefix + "_" + persistenceIdPostfix.apply((C) command);

	}

	/**
	 * Returns the shard on which the given entityId should be placed
	 */
	public String getShardId(String entityId) {
		return String.valueOf(entityId.hashCode() % numberOfShards);
	}


}
