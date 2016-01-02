package com.puresoltechnologies.ductiledb.core.manager;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;

public class DuctileDBGraphManagerImpl implements DuctileDBGraphManager {

    private final DuctileDBGraphImpl ductileDBGraphImpl;

    public DuctileDBGraphManagerImpl(DuctileDBGraphImpl ductileDBGraphImpl) {
	super();
	this.ductileDBGraphImpl = ductileDBGraphImpl;
    }

    @Override
    public DuctileDBGraph getGraph() {
	return ductileDBGraphImpl;
    }

    @Override
    public Iterable<String> getVariableNames() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public <T> void setVariable(String variableName, T value) {
	// TODO Auto-generated method stub

    }

    @Override
    public <T> T getVariable(String variableName) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void removeVariable(String variableName) {
	// TODO Auto-generated method stub

    }

}
