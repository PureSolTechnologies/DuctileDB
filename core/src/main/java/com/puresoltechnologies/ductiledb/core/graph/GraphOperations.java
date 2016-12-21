package com.puresoltechnologies.ductiledb.core.graph;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.blobstore.BlobStore;
import com.puresoltechnologies.ductiledb.core.graph.tx.DuctileDBTransaction;

/**
 * This interface defines all public operations on a Ductile Graph. The
 * interface is used to define the {@link GraphStore} and also the
 * transaction {@link DuctileDBTransaction}.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface GraphOperations {

    /**
     * This method adds a new vertex with a new internal id. There are neither
     * types nor properties set, yet.
     * 
     * @return A {@link DuctileDBVertex} is returned containing the vertex
     *         content.
     */
    public default DuctileDBVertex addVertex() {
	return addVertex(new HashSet<>(), new HashMap<>());
    }

    /**
     * This method adds a new vertex with a new internal id. The types and
     * properties can be predefined, so that the newly created vertex contains
     * all information already. This method is to be used in favor over
     * {@link #addVertex()} for performance.
     * 
     * @param types
     *            is a {@link Set} of {@link String} containing the types to be
     *            set for initial creation.
     * @param properties
     *            is a {@link Map} of {@link String} and {@link Object} to
     *            define the properties which are to be set initially.
     * @return A {@link DuctileDBVertex} is returned containing the vertex
     *         content.
     */
    public DuctileDBVertex addVertex(Set<String> types, Map<String, Object> properties);

    /**
     * This method adds a new BLOB vertex with a new internal id. There are
     * neither types nor properties set, yet. Only the content of the BLOB is
     * provided to be stored in {@link BlobStore}.
     * 
     * @param blobContent
     *            is an {@link InputStream} which is used to provide the BLOB
     *            content to be stored.
     * @return A {@link DuctileDBVertex} is returned containing the vertex
     *         content.
     */
    public default DuctileDBVertex addBlobVertex(InputStream blobContent) {
	return addBlobVertex(blobContent, new HashSet<>(), new HashMap<>());
    }

    /**
     * This method adds a new BLOB vertex with a new internal id. The types and
     * properties can be predefined, so that the newly created vertex contains
     * all information already. This method is to be used in favor over
     * {@link #addBlobVertex(InputStream)} for performance.
     * 
     * @param blobContent
     *            is an {@link InputStream} which is used to provide the BLOB
     *            content to be stored.
     * @param types
     *            is a {@link Set} of {@link String} containing the types to be
     *            set for initial creation.
     * @param properties
     *            is a {@link Map} of {@link String} and {@link Object} to
     *            define the properties which are to be set initially.
     * @return A {@link DuctileDBVertex} is returned containing the vertex
     *         content.
     */
    public DuctileDBVertex addBlobVertex(InputStream blobContent, Set<String> types, Map<String, Object> properties);

    /**
     * This method returns the vertex defined via its id.
     * 
     * @param vertexId
     *            is the id of the vertex which is to be returned.
     * @return A {@link DuctileDBVertex} is returned containing the loaded
     *         vertex. <code>null</code> is returned in case no vertex with the
     *         given id was found.
     */
    public DuctileDBVertex getVertex(long vertexId);

    /**
     * The given vertex is to be removed from the graph. All its edges and index
     * entries are also removed.
     * 
     * @param vertex
     *            is the {@link DuctileDBVertex} to be removed.
     */
    public void removeVertex(DuctileDBVertex vertex);

    /**
     * The method returns a list of <b>all(!)</b> vertices in the graph. This is
     * only to be called on small graphs, for testing or if really needed,
     * because memory usage may be huge and time consuming.
     * 
     * @return Returns an {@link Iterable} of {@link DuctileDBVertex} containing
     *         the result vertices.
     */
    public Iterable<DuctileDBVertex> getVertices();

    /**
     * This method returns all vertices with a given type.
     * 
     * @param type
     *            is the type to be looked up.
     * @return Returns an {@link Iterable} of {@link DuctileDBVertex} containing
     *         the result vertices.
     */
    public Iterable<DuctileDBVertex> getVertices(String type);

    /**
     * This method returns all vertices with a given property.
     * 
     * @param propertyKey
     *            is the key of the property to check for.
     * @param propertyValue
     *            is the value to be looked up. This value may be
     *            <code>null</code> to provide a list of all vertices which have
     *            a certain property applied to it (has property).
     * @return Returns an {@link Iterable} of {@link DuctileDBVertex} containing
     *         the result vertices.
     */
    public Iterable<DuctileDBVertex> getVertices(String propertyKey, Object propertyValue);

    /**
     * This method adds a new edge to the graph.
     * 
     * @param startVertex
     *            is a {@link DuctileDBVertex} where the edge is starting
     *            (outgoing).
     * @param targetVertex
     *            is a {@link DuctileDBVertex} where the edge is ending
     *            (incoming).
     * @param type
     *            is the type (or label) of the edge as {@link String}.
     * @return A {@link DuctileDBEdge} object is returned containing the newly
     *         created edge.
     */
    public default DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String type) {
	return addEdge(startVertex, targetVertex, type, new HashMap<>());
    }

    /**
     * This method adds a new edge to the graph. This method is used in favor
     * over {@link #addEdge(DuctileDBVertex, DuctileDBVertex, String)} due to
     * performance reasons.
     * 
     * @param startVertex
     *            is a {@link DuctileDBVertex} where the edge is starting
     *            (outgoing).
     * @param targetVertex
     *            is a {@link DuctileDBVertex} where the edge is ending
     *            (incoming).
     * @param type
     *            is the type (or label) of the edge as {@link String}.
     * @param properties
     *            is a {@link Map} containing the properties to be set during
     *            creation of the edge.
     * @return A {@link DuctileDBEdge} object is returned containing the newly
     *         created edge.
     */
    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String type,
	    Map<String, Object> properties);

    /**
     * This method returns the edge with the given id.
     * 
     * @param edgeId
     *            is the id of the edge to be loaded.
     * @return A {@link DuctileDBEdge} object is returned with the loaded edge.
     *         <code>null</code> is returned in case no edge with the given id
     *         was found.
     */
    public DuctileDBEdge getEdge(long edgeId);

    /**
     * Removes a provided edge. The index entries for the edge are removed, too.
     * The start and target vertex are not altered, only the reference of the
     * edge is removed.
     * 
     * @param edge
     *            is the {@link DuctileDBEdge} to be removed from graph.
     */
    public void removeEdge(DuctileDBEdge edge);

    /**
     * This method returns <b>all(!)</b> edges in the graph. This is only to be
     * called on small graphs, for testing or if really needed, because memory
     * usage may be huge and time consuming.
     * 
     * @return An {@link Iterable} of {@link DuctileDBEdge} is returned
     *         containing the result edges.
     */
    public Iterable<DuctileDBEdge> getEdges();

    /**
     * This method is used to get all edges of a certain type (with a certain
     * label).
     * 
     * @param type
     *            is the type name of the edges to be returned.
     * @return An {@link Iterable} of {@link DuctileDBEdge} is returned
     *         containing the result edges.
     */
    public Iterable<DuctileDBEdge> getEdges(String type);

    /**
     * This method is used to get all edges with a certain property.
     * 
     * @param propertyKey
     *            is the property to look up.
     * @param propertyValue
     *            is the value to be checked for. This value may be
     *            <code>null</code> to look for all properties with the given
     *            key independently of their value (has property check).
     * @return An {@link Iterable} of {@link DuctileDBEdge} is returned
     *         containing the result edges.
     */
    public Iterable<DuctileDBEdge> getEdges(String propertyKey, Object propertyValue);
}
