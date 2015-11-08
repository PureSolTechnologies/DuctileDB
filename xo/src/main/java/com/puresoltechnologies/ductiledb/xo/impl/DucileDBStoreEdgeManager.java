package com.puresoltechnologies.ductiledb.xo.impl;

import java.util.Iterator;
import java.util.Map;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.DatastoreRelationManager;
import com.buschmais.xo.spi.metadata.method.PrimitivePropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.type.RelationTypeMetadata;
import com.puresoltechnologies.ductiledb.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBPropertyMetadata;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * This class implements the XO DatastorePropertyManager for Titan database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DucileDBStoreEdgeManager implements
	DatastoreRelationManager<DuctileDBVertex, Long, DuctileDBEdge, DuctileDBEdgeMetadata, String, DuctileDBPropertyMetadata> {

    private final DuctileDBGraph graph;

    DucileDBStoreEdgeManager(DuctileDBGraph graph) {
	this.graph = graph;
    }

    @Override
    public boolean hasSingleRelation(DuctileDBVertex source, RelationTypeMetadata<DuctileDBEdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction) {
	String label = metadata.getDatastoreMetadata().getDiscriminator();
	long count;
	switch (direction) {
	case FROM:
	    count = source.query().direction(Direction.OUT).labels(label).count();
	    break;
	case TO:
	    count = source.query().direction(Direction.IN).labels(label).count();
	    break;
	default:
	    throw new XOException("Unkown direction '" + direction.name() + "'.");
	}
	if (count > 1) {
	    throw new XOException("Multiple results are available.");
	}
	return count == 1;
    }

    @Override
    public DuctileDBEdge getSingleRelation(DuctileDBVertex source, RelationTypeMetadata<DuctileDBEdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction) {
	String descriminator = metadata.getDatastoreMetadata().getDiscriminator();
	Iterable<Edge> edges;
	switch (direction) {
	case FROM:
	    edges = source.getEdges(Direction.OUT, descriminator);
	    break;
	case TO:
	    edges = source.getEdges(Direction.IN, descriminator);
	    break;
	default:
	    throw new XOException("Unkown direction '" + direction.name() + "'.");
	}
	Iterator<Edge> iterator = edges.iterator();
	if (!iterator.hasNext()) {
	    throw new XOException("No result is available.");
	}
	DuctileDBEdge result = (DuctileDBEdge) iterator.next();
	if (iterator.hasNext()) {
	    throw new XOException("Multiple results are available.");
	}
	return result;
    }

    @Override
    public Iterable<DuctileDBEdge> getRelations(DuctileDBVertex source,
	    RelationTypeMetadata<DuctileDBEdgeMetadata> metadata, RelationTypeMetadata.Direction direction) {
	VertexQuery query = source.query();
	String discriminator = metadata.getDatastoreMetadata().getDiscriminator();
	switch (direction) {
	case TO:
	    query = query.direction(Direction.IN).labels(discriminator);
	    break;
	case FROM:
	    query = query.direction(Direction.OUT).labels(discriminator);
	    break;
	default:
	    throw new XOException("Unknown direction '" + direction.name() + "'.");
	}
	return query.edges();
    }

    @Override
    public DuctileDBEdge createRelation(DuctileDBVertex source, RelationTypeMetadata<DuctileDBEdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction, DuctileDBVertex target,
	    Map<PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata>, Object> exampleEntity) {
	String name = metadata.getDatastoreMetadata().getDiscriminator();
	switch (direction) {
	case FROM:
	    return source.addEdge(name, target);
	case TO:
	    return target.addEdge(name, source);
	default:
	    throw new XOException("Unknown direction '" + direction.name() + "'.");
	}
    }

    @Override
    public void deleteRelation(DuctileDBEdge edge) {
	edge.remove();
    }

    @Override
    public DuctileDBVertex getTo(DuctileDBEdge edge) {
	return edge.getVertex(Direction.IN);
    }

    @Override
    public DuctileDBVertex getFrom(DuctileDBEdge edge) {
	return edge.getVertex(Direction.OUT);
    }

    @Override
    public void setProperty(DuctileDBEdge edge, PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata,
	    Object value) {
	edge.setProperty(metadata.getDatastoreMetadata().getName(), value);
    }

    @Override
    public boolean hasProperty(DuctileDBEdge edge,
	    PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	return edge.getProperty(metadata.getDatastoreMetadata().getName()) != null;
    }

    @Override
    public void removeProperty(DuctileDBEdge edge,
	    PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	edge.removeProperty(metadata.getDatastoreMetadata().getName());
    }

    @Override
    public Object getProperty(DuctileDBEdge edge, PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	return edge.getProperty(metadata.getDatastoreMetadata().getName());
    }

    @Override
    public boolean isRelation(Object o) {
	return Edge.class.isAssignableFrom(o.getClass());
    }

    @Override
    public String getRelationDiscriminator(DuctileDBEdge edge) {
	return edge.getLabel();
    }

    @Override
    public Long getRelationId(DuctileDBEdge edge) {
	return edge.getId();
    }

    @Override
    public void flushRelation(DuctileDBEdge edge) {
	// intentionally left empty
    }

    @Override
    public DuctileDBEdge findRelationById(RelationTypeMetadata<DuctileDBEdgeMetadata> metadata, Long id) {
	return graph.getEdge(id);
    }

}
