package com.spring.akka.eventsourcing.persistance;

import java.io.Serializable;

/**
 * the message trigger type to inform the persistent actor to resume processing and stop stashing messages
 */
public class ResumeProcessing implements Serializable {
}
