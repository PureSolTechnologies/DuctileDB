package com.puresoltechnologies.ductiledb.api.manager;

import java.io.Serializable;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.versioning.Version;

/**
 * The graph manager is used to configure the graph and define its schema.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBGraphManager {

    /**
     * This method returns the graph for which this manager is responsible for.
     * 
     * @return A {@link DuctileDBGraph} is returned.
     */
    public DuctileDBGraph getGraph();

    /**
     * This method returns the version of the current implementation.
     * 
     * @return A {@link Version} object is returned containing the current
     *         version of DuctileDB.
     */
    public Version getVersion();

    /**
     * This method returns all variable names of the graph.
     * 
     * @return An {@link Iterable} is returned containing the names of the
     *         variables.
     */
    public Iterable<String> getVariableNames();

    /**
     * This method sets a new value to a graph variable.
     * 
     * @param variableName
     *            is the name of the variable.
     * @param value
     *            is the value to be set.
     */
    public <T extends Serializable> void setVariable(String variableName, T value);

    /**
     * This method returns the variable value of a variable.
     * 
     * @param variableName
     *            is the name of the variable.
     * @return A value is returned currently assigned to this variable.
     *         <code>null</code> is returned in case the variable is not set.
     */
    public <T> T getVariable(String variableName);

    /**
     * This method is used to remove a variable from the graph.
     * 
     * @param variableName
     */
    public void removeVariable(String variableName);
}
