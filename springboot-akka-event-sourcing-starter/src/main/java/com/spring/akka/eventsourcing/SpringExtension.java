package com.spring.akka.eventsourcing;

import java.util.Collections;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import akka.actor.AbstractActor;
import akka.actor.AbstractExtensionId;
import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.actor.Extension;
import akka.actor.Props;
import akka.routing.RouterConfig;

/**
 * An Akka Extension to provide access to Spring managed Actor Beans.
 */
@Service
public class SpringExtension extends AbstractExtensionId<SpringExtension.SpringExt> {

	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	private ActorSystem actorSystem;

	@PostConstruct
	public void postConstruct() {
		this.get(actorSystem).initialize(applicationContext);
	}

	/**
	 * Is used by Akka to instantiate the Extension identified by this
	 * ExtensionId, internal use only.
	 */
	@Override
	public SpringExt createExtension(ExtendedActorSystem system) {
		return new SpringExt();
	}

	public ActorRef actorOf(ActorContext actorContext, String actorSpringBeanName, String actorLogicalName, Object... actorParameters) {
		return actorContext.actorOf(get(actorContext.system()).props(actorSpringBeanName, actorParameters), actorLogicalName);
	}

	public ActorRef actorOf(ActorContext actorContext, Class<?> requiredType, String actorLogicalName, Object... actorParameters) {
		return actorContext.actorOf(get(actorContext.system()).props(requiredType, actorParameters), actorLogicalName);
	}

	public ActorRef actorOf(ActorContext actorContext, String actorSpringBeanName, String actorLogicalName, Class<?> requiredType) {
		return actorContext.actorOf(get(actorContext.system()).props(actorSpringBeanName, requiredType), actorLogicalName);
	}

	public ActorRef actorOf(ActorContext actorContext, String actorSpringBeanName, Object... actorParameters) {
		return actorContext.actorOf(get(actorContext.system()).props(actorSpringBeanName, actorParameters));
	}

	public ActorRef actorOf(ActorSystem actorSystem, String actorSpringBeanName, Object... actorParameters) {
		return actorSystem.actorOf(get(actorSystem).props(actorSpringBeanName, actorParameters));
	}

	public ActorRef actorOf(ActorSystem actorSystem, String actorSpringBeanName) {
		return actorSystem.actorOf(get(actorSystem).props(actorSpringBeanName));
	}

	public ActorRef actorOf(ActorSystem actorSystem, String actorSpringBeanName, RouterConfig routerConfig, String dispatcher, String actorLogicalName) {
		return actorSystem.actorOf(get(actorSystem).props(actorSpringBeanName).withRouter(routerConfig).withDispatcher(dispatcher), actorLogicalName);

	}

	public ActorRef actorOf(ActorSystem actorSystem, String actorSpringBeanName, RouterConfig routerConfig, String dispatcher) {
		return actorSystem.actorOf(get(actorSystem).props(actorSpringBeanName).withRouter(routerConfig).withDispatcher(dispatcher));

	}

	public ActorRef actorOf(ActorSystem actorSystem, String actorSpringBeanName, RouterConfig routerConfig, String dispatcher, Object... actorParameters) {
		return actorSystem.actorOf(get(actorSystem).props(actorSpringBeanName, actorParameters).withRouter(routerConfig).withDispatcher(dispatcher));

	}

	public ActorRef actorOf(ActorSystem actorSystem, String actorSpringBeanName, RouterConfig routerConfig, String dispatcher, String actorLogicalName, Object... actorParameters) {
		return actorSystem.actorOf(get(actorSystem).props(actorSpringBeanName, actorParameters).withRouter(routerConfig).withDispatcher(dispatcher), actorLogicalName);

	}

	public ActorRef actorOf(ActorSystem actorSystem, String actorSpringBeanName, String actorLogicalName, Object... actorParameters) {
		return actorSystem.actorOf(get(actorSystem).props(actorSpringBeanName, actorParameters), actorLogicalName);

	}


	/**
	 * The Extension implementation.
	 */
	public static class SpringExt implements Extension {
		private volatile ApplicationContext applicationContext;

		/**
		 * Used to initialize the Spring application context for the extension.
		 *
		 * @param applicationContext - spring application context.
		 */
		public void initialize(ApplicationContext applicationContext) {
			this.applicationContext = applicationContext;
		}


		/**
		 * Create a Props for the specified actorBeanName using the
		 * SpringActorProducer class.
		 *
		 * @param actorBeanName The name of the actor bean to create Props for
		 * @return a Props that will create the named actor bean using Spring
		 */
		public Props props(String actorBeanName) {
			return props(actorBeanName, Collections.emptyList());
		}

		/**
		 * Create a Props for the specified actorBeanName using the
		 * SpringActorProducer class.
		 *
		 * @param actorBeanName The name of the actor bean to create Props for
		 * @param parameters    If any parameters this Actor needs, pass null if no parameters.
		 * @return a Props that will create the named actor bean using Spring
		 */
		public Props props(String actorBeanName, Object... parameters) {
			return (parameters != null && parameters.length > 0) ? Props.create(SpringActorProducer.class, applicationContext,
					actorBeanName,
					parameters) : Props.create(SpringActorProducer.class,
					applicationContext,
					actorBeanName);


		}

		/**
		 * Create a Props for the specified actorBeanName using the SpringActorProducer class.
		 *
		 * @param requiredType Type of the actor bean must match. Can be an interface or superclass of the actual class,
		 *                     or {@code null} for any match. For example, if the value is {@code Object.class}, this method will succeed
		 *                     whatever the class of the returned instance.
		 * @return a Props that will create the actor bean using Spring
		 */
		public Props props(Class<?> requiredType, Object... args) {
			return (args != null && args.length > 0) ? Props.create(SpringActorProducer.class, applicationContext,
					requiredType,
					args) : Props.create(SpringActorProducer.class,
					applicationContext,
					requiredType);
		}

		/**
		 * Create a Props for the specified actorBeanName using the SpringActorProducer class.
		 *
		 * @param actorBeanName The name of the actor bean to create Props for
		 * @param requiredType  Type of the actor bean must match. Can be an interface or superclass of the actual class,
		 *                      or {@code null} for any match. For example, if the value is {@code Object.class}, this method will succeed
		 *                      whatever the class of the returned instance.
		 * @return a Props that will create the actor bean using Spring
		 */
		public Props props(String actorBeanName, Class<? extends AbstractActor> requiredType) {
			return Props.create(SpringActorProducer.class, applicationContext, actorBeanName, requiredType);
		}
	}
}
