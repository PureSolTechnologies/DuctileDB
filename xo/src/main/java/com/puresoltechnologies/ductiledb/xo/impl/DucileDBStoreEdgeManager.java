package com.puresoltechnologies.ductiledb.xo.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.DatastoreRelationManager;
import com.buschmais.xo.spi.metadata.method.PrimitivePropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.type.RelationTypeMetadata;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBPropertyMetadata;

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
	    count = source.query().direction(EdgeDirection.OUT).labels(label).count();
	    break;
	case TO:
	    count = source.query().direction(EdgeDirection.IN).labels(label).count();
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
    public DuctileDBEdge getSingleRelation(DuctileDBVertex source, RelationTypeMetadata<EdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction) {
	String descriminator = metadata.getDatastoreMetadata().getDiscriminator();
	Iterable<DuctileDBEdge> edges;
	switch (direction) {
	case FROM:
	    edges = source.getEdges(EdgeDirection.OUT, descriminator);
	    break;
	case TO:
	    edges = source.getEdges(EdgeDirection.IN, descriminator);
	    break;
	default:
	    throw new XOException("Unkown direction '" + direction.name() + "'.");
	}
	Iterator<DuctileDBEdge> iterator = edges.iterator();
	if (!iterator.hasNext()) {
	    throw new XOException("No result is available.");
	}
	DuctileDBEdge result = iterator.next();
	if (iterator.hasNext()) {
	    throw new XOException("Multiple results are available.");
	}
	return result;
    }

    @Override
    public Iterable<DuctileDBEdge> getRelations(DuctileDBVertex source, RelationTypeMetadata<EdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction) {
	String discriminator = metadata.getDatastoreMetadata().getDiscriminator();
	Iterable<DuctileDBEdge> edges = null;
	switch (direction) {
	case TO:
	    edges = source.getEdges(EdgeDirection.IN, discriminator);
	    break;
	case FROM:
	    edges = source.getEdges(EdgeDirection.OUT, discriminator);
	    break;
	default:
	    throw new XOException("Unknown direction '" + direction.name() + "'.");
	}
	List<DuctileDBEdge> result = new ArrayList<>();
	for (DuctileDBEdge edge : edges) {
	    result.add(edge);
	}
	return new Iterable<DuctileDBEdge>() {
	    @Override
	    public Iterator<DuctileDBEdge> iterator() {
		return result.iterator();
	    }
	};
    }

    @Override
    public DuctileDBEdge createRelation(DuctileDBVertex source, RelationTypeMetadata<EdgeMetadata> metadata,
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
	return edge.getVertex(EdgeDirection.IN);
    }

    @Override
    public DuctileDBVertex getFrom(DuctileDBEdge edge) {
	return edge.getVertex(EdgeDirection.OUT);
    }

    @Override
    public void setProperty(DuctileDBEdge edge, PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata,
	    Object value) {
	edge.setProperty(metadata.getDatastoreMetadata().getName(), value);
    }

    @Override
    public boolean hasProperty(DuctileDBEdge edge, PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	return edge.getProperty(metadata.getDatastoreMetadata().getName()) != null;
    }

    @Override
    public void removeProperty(DuctileDBEdge edge, PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	edge.removeProperty(metadata.getDatastoreMetadata().getName());
    }

    @Override
    public Object getProperty(DuctileDBEdge edge, PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	return edge.getProperty(metadata.getDatastoreMetadata().getName());
    }

    @Override
    public boolean isRelation(Object o) {
	return DuctileDBEdge.class.isAssignableFrom(o.getClass());
    }

    @Override
    public String getRelationDiscriminator(DuctileDBEdge edge) {
	return edge.getType();
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
