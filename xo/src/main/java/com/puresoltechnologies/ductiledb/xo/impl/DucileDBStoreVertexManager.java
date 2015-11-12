package com.puresoltechnologies.ductiledb.xo.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.buschmais.xo.api.ResultIterator;
import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.DatastoreEntityManager;
import com.buschmais.xo.spi.datastore.TypeMetadataSet;
import com.buschmais.xo.spi.metadata.method.IndexedPropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.method.PrimitivePropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.type.EntityTypeMetadata;
import com.puresoltechnologies.ductiledb.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBPropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBVertexMetadata;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

/**
 * This class implements the XO DatastorePropertyManager for Titan database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DucileDBStoreVertexManager implements
	DatastoreEntityManager<Long, DuctileDBVertex, DuctileDBVertexMetadata, String, DuctileDBPropertyMetadata> {

    private final DuctileDBGraph graph;

    DucileDBStoreVertexManager(DuctileDBGraph graph) {
	this.graph = graph;
    }

    @Override
    public void setProperty(DuctileDBVertex vertex, PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata,
	    Object value) {
	vertex.setProperty(metadata.getDatastoreMetadata().getName(), value);
    }

    @Override
    public boolean hasProperty(DuctileDBVertex vertex,
	    PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	return vertex.getProperty(metadata.getDatastoreMetadata().getName()) != null;
    }

    @Override
    public void removeProperty(DuctileDBVertex vertex,
	    PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	vertex.removeProperty(metadata.getDatastoreMetadata().getName());
    }

    @Override
    public Object getProperty(DuctileDBVertex vertex,
	    PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> metadata) {
	return vertex.getProperty(metadata.getDatastoreMetadata().getName());
    }

    @Override
    public boolean isEntity(Object o) {
	return Vertex.class.isAssignableFrom(o.getClass());
    }

    @Override
    public Set<String> getEntityDiscriminators(DuctileDBVertex vertex) {
	Set<String> discriminators = new HashSet<>();
	for (String key : vertex.getPropertyKeys()) {
	    if (key.startsWith(DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY)) {
		String discriminator = vertex.getProperty(key);
		discriminators.add(discriminator);
	    }
	}
	if (discriminators.size() == 0) {
	    throw new XOException(
		    "A vertex was found without discriminators. Does another framework alter the database?");
	}
	return discriminators;
    }

    @Override
    public Long getEntityId(DuctileDBVertex vertex) {
	return vertex.getId();
    }

    @Override
    public DuctileDBVertex createEntity(TypeMetadataSet<EntityTypeMetadata<DuctileDBVertexMetadata>> types,
	    Set<String> discriminators,
	    Map<PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata>, Object> exampleEntity) {
	DuctileDBVertex vertex = graph.addVertex();
	for (String discriminator : discriminators) {
	    vertex.setProperty(DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + discriminator, discriminator);
	}
	return vertex;
    }

    @Override
    public void deleteEntity(DuctileDBVertex vertex) {
	vertex.remove();
    }

    @Override
    public ResultIterator<DuctileDBVertex> findEntity(EntityTypeMetadata<DuctileDBVertexMetadata> type,
	    String discriminator, Map<PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata>, Object> values) {
	if (values.size() > 1) {
	    throw new XOException("Only one property value is supported for find operation");
	}
	GraphQuery query = graph.query();
	query = query.has(DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + discriminator);

	IndexedPropertyMethodMetadata<?> indexedProperty = type.getDatastoreMetadata().getIndexedProperty();
	if (indexedProperty == null) {
	    indexedProperty = type.getIndexedProperty();
	}
	if (indexedProperty == null) {
	    throw new XOException(
		    "Type " + type.getAnnotatedType().getAnnotatedElement().getName() + " has no indexed property.");
	}
	PrimitivePropertyMethodMetadata<DuctileDBPropertyMetadata> propertyMethodMetadata = indexedProperty
		.getPropertyMethodMetadata();
	String name = propertyMethodMetadata.getDatastoreMetadata().getName();
	query = query.has(name, values.values().iterator().next());
	Iterable<Vertex> vertices = query.vertices();
	final Iterator<Vertex> iterator = vertices.iterator();

	return new ResultIterator<DuctileDBVertex>() {

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public DuctileDBVertex next() {
		return (DuctileDBVertex) iterator.next();
	    }

	    @Override
	    public void remove() {
		iterator.remove();
	    }

	    @Override
	    public void close() {
		// intentionally left empty
	    }
	};
    }

    @Override
    public DuctileDBVertex findEntityById(EntityTypeMetadata<DuctileDBVertexMetadata> metadata, String discriminator,
	    Long id) {
	return graph.getVertex(id);
    }

    @Override
    public void migrateEntity(DuctileDBVertex vertex,
	    TypeMetadataSet<EntityTypeMetadata<DuctileDBVertexMetadata>> types, Set<String> discriminators,
	    TypeMetadataSet<EntityTypeMetadata<DuctileDBVertexMetadata>> targetTypes,
	    Set<String> targetDiscriminators) {
	for (String discriminator : discriminators) {
	    if (!targetDiscriminators.contains(discriminator)) {
		vertex.removeProperty(DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + discriminator);
	    }
	}
	for (String discriminator : targetDiscriminators) {
	    if (!discriminators.contains(discriminator)) {
		vertex.setProperty(DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + discriminator, discriminator);
	    }
	}
    }

    @Override
    public void flushEntity(DuctileDBVertex vertex) {
	// intentionally left empty
    }

    @Override
    public void addDiscriminators(DuctileDBVertex entity, Set<String> discriminators) {
	for (String dicriminator : discriminators) {
	    entity.addLabel(dicriminator);
	}
    }

    @Override
    public void removeDiscriminators(DuctileDBVertex entity, Set<String> discriminators) {
	for (String dicriminator : discriminators) {
	    entity.removeLabel(dicriminator);
	}
    }

}
