package com.puresoltechnologies.ductiledb.xo.impl;

import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.DatastoreRelationManager;
import com.buschmais.xo.spi.metadata.method.PrimitivePropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.type.RelationTypeMetadata;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileEdge;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctilePropertyMetadata;

/**
 * This class implements the XO DatastorePropertyManager for DuctileDB database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DucileStoreEdgeManager implements
	DatastoreRelationManager<DuctileVertex, Long, DuctileEdge, DuctileEdgeMetadata, String, DuctilePropertyMetadata> {

    private final DuctileGraph graph;

    DucileStoreEdgeManager(DuctileGraph graph) {
	this.graph = graph;
    }

    @Override
    public boolean hasSingleRelation(DuctileVertex source, RelationTypeMetadata<DuctileEdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction) {
	String label = metadata.getDatastoreMetadata().getDiscriminator();
	long count;
	switch (direction) {
	case FROM:
	    count = graph.traversal().V(source.id()).outE(label).count().next();
	    break;
	case TO:
	    count = graph.traversal().V(source.id()).inE(label).count().next();
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
    public DuctileEdge getSingleRelation(DuctileVertex source, RelationTypeMetadata<DuctileEdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction) {
	String descriminator = metadata.getDatastoreMetadata().getDiscriminator();
	Iterator<Edge> edges;
	switch (direction) {
	case FROM:
	    edges = source.edges(Direction.OUT, descriminator);
	    break;
	case TO:
	    edges = source.edges(Direction.IN, descriminator);
	    break;
	default:
	    throw new XOException("Unkown direction '" + direction.name() + "'.");
	}
	if (!edges.hasNext()) {
	    throw new XOException("No result is available.");
	}
	DuctileEdge result = (DuctileEdge) edges.next();
	if (edges.hasNext()) {
	    throw new XOException("Multiple results are available.");
	}
	return result;
    }

    @Override
    public Iterable<DuctileEdge> getRelations(DuctileVertex source, RelationTypeMetadata<DuctileEdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction) {
	String discriminator = metadata.getDatastoreMetadata().getDiscriminator();
	final Iterator<Edge> edges;
	switch (direction) {
	case TO:
	    edges = source.edges(Direction.IN, discriminator);
	    break;
	case FROM:
	    edges = source.edges(Direction.OUT, discriminator);
	    break;
	default:
	    throw new XOException("Unknown direction '" + direction.name() + "'.");
	}
	return new Iterable<DuctileEdge>() {
	    @Override
	    public Iterator<DuctileEdge> iterator() {
		return new CastIterator<Edge, DuctileEdge>(edges);
	    }
	};
    }

    @Override
    public DuctileEdge createRelation(DuctileVertex source, RelationTypeMetadata<DuctileEdgeMetadata> metadata,
	    RelationTypeMetadata.Direction direction, DuctileVertex target,
	    Map<PrimitivePropertyMethodMetadata<DuctilePropertyMetadata>, Object> exampleEntity) {
	String name = metadata.getDatastoreMetadata().getDiscriminator();
	Object[] keyValues = new Object[exampleEntity.size() * 2];
	@SuppressWarnings("unchecked")
	PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> keys[] = exampleEntity.keySet()
		.toArray(new PrimitivePropertyMethodMetadata[exampleEntity.size()]);
	for (int i = 0; i < exampleEntity.size(); ++i) {
	    PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> key = keys[i];
	    String propertyName = key.getDatastoreMetadata().getName();
	    keyValues[2 * i] = propertyName;
	    keyValues[2 * i + 1] = exampleEntity.get(key);
	}
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
    public void deleteRelation(DuctileEdge edge) {
	edge.remove();
    }

    @Override
    public DuctileVertex getTo(DuctileEdge edge) {
	return edge.inVertex();
    }

    @Override
    public DuctileVertex getFrom(DuctileEdge edge) {
	return edge.outVertex();
    }

    @Override
    public void setProperty(DuctileEdge edge, PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> metadata,
	    Object value) {
	edge.property(metadata.getDatastoreMetadata().getName(), value);
    }

    @Override
    public boolean hasProperty(DuctileEdge edge, PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> metadata) {
	return edge.property(metadata.getDatastoreMetadata().getName()) != null;
    }

    @Override
    public void removeProperty(DuctileEdge edge, PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> metadata) {
	edge.property(metadata.getDatastoreMetadata().getName()).remove();
    }

    @Override
    public Object getProperty(DuctileEdge edge, PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> metadata) {
	Property<Object> property = edge.property(metadata.getDatastoreMetadata().getName());
	if (!property.isPresent()) {
	    return null;
	}
	return property.value();
    }

    @Override
    public boolean isRelation(Object o) {
	return DuctileEdge.class.isAssignableFrom(o.getClass());
    }

    @Override
    public String getRelationDiscriminator(DuctileEdge edge) {
	return edge.label();
    }

    @Override
    public Long getRelationId(DuctileEdge edge) {
	return edge.id();
    }

    @Override
    public void flushRelation(DuctileEdge edge) {
	// intentionally left empty
    }

    @Override
    public DuctileEdge findRelationById(RelationTypeMetadata<DuctileEdgeMetadata> metadata, Long id) {
	Iterator<Edge> edges = graph.edges(id);
	if (!edges.hasNext()) {
	    return null;
	}
	return (DuctileEdge) edges.next();
    }

}
