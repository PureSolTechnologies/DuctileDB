package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class DuctileMessageBoard<M> {

    private Map<Vertex, Queue<M>> sendMessages = new ConcurrentHashMap<>();
    private Map<Vertex, Queue<M>> receiveMessages = new ConcurrentHashMap<>();
    private Set<MessageScope> previousMessageScopes = new HashSet<>();
    private Set<MessageScope> currentMessageScopes = new HashSet<>();

    public void completeIteration() {
	receiveMessages = sendMessages;
	sendMessages = new ConcurrentHashMap<>();
	previousMessageScopes = currentMessageScopes;
	currentMessageScopes = new HashSet<>();
    }

    public Set<MessageScope> getPreviousMessageScopes() {
	return Collections.unmodifiableSet(previousMessageScopes);
    }

    public Queue<M> getReceiveMessage(Vertex next) {
	return receiveMessages.get(next);
    }

    public Map<Vertex, Queue<M>> getReceiveMessages() {
	return receiveMessages;
    }

    public void addCurrentMessageScope(MessageScope messageScope) {
	currentMessageScopes.add(messageScope);

    }

    public Map<Vertex, Queue<M>> getSendMessages() {
	return sendMessages;
    }

}
