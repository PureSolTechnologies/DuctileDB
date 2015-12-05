package com.puresoltechnologies.ductiledb.core;

import java.util.Set;

import com.puresoltechnologies.ductiledb.api.DuctileDBElement;

public class DuctileDBElementImpl implements DuctileDBElement {

    private final DuctileDBGraphImpl graph;
    private final long id;

    public DuctileDBElementImpl(DuctileDBGraphImpl graph, long id) {
	super();
	this.graph = graph;
	this.id = id;
    }

    protected DuctileDBGraphImpl getGraph() {
	return graph;
    }

    @Override
    public final Long getId() {
	return id;
    }

    @Override
    public Set<String> getPropertyKeys() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void setProperty(String key, Object value) {
	// TODO Auto-generated method stub

    }

    @Override
    public <T> T getProperty(String key) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public <T> T removeProperty(String key) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void remove() {
	// TODO Auto-generated method stub

    }

}
