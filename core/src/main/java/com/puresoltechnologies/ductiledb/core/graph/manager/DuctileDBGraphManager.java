package com.puresoltechnologies.ductiledb.core.graph.manager;

import java.io.Serializable;

import com.puresoltechnologies.ductiledb.core.graph.GraphStore;
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
     * @return A {@link GraphStore} is returned.
     */
    public GraphStore getGraph();

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
     * @param <T>
     *            is the actual type of the variable.
     */
    public <T extends Serializable> void setVariable(String variableName, T value);

    /**
     * This method returns the variable value of a variable.
     * 
     * @param variableName
     *            is the name of the variable.
     * @param <T>
     *            is the actual type of the variable.
     * @return A value is returned currently assigned to this variable.
     *         <code>null</code> is returned in case the variable is not set.
     */
    public <T> T getVariable(String variableName);

    /**
     * This method is used to remove a variable from the graph.
     * 
     * @param variableName
     *            is the name of the variable to be removed.
     */
    public void removeVariable(String variableName);
}
