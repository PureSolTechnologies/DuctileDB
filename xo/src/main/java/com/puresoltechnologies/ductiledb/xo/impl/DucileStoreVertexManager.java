package com.puresoltechnologies.ductiledb.xo.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.buschmais.xo.api.ResultIterator;
import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.DatastoreEntityManager;
import com.buschmais.xo.spi.datastore.TypeMetadataSet;
import com.buschmais.xo.spi.metadata.method.IndexedPropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.method.PrimitivePropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.type.EntityTypeMetadata;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctilePropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileVertexMetadata;

/**
 * This class implements the XO DatastorePropertyManager for Titan database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DucileStoreVertexManager
	implements DatastoreEntityManager<Long, DuctileVertex, DuctileVertexMetadata, String, DuctilePropertyMetadata> {

    private final DuctileGraph graph;

    DucileStoreVertexManager(DuctileGraph graph) {
	this.graph = graph;
    }

    @Override
    public void setProperty(DuctileVertex vertex, PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> metadata,
	    Object value) {
	vertex.property(metadata.getDatastoreMetadata().getName(), value);
    }

    @Override
    public boolean hasProperty(DuctileVertex vertex,
	    PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> metadata) {
	return vertex.property(metadata.getDatastoreMetadata().getName()) != null;
    }

    @Override
    public void removeProperty(DuctileVertex vertex,
	    PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> metadata) {
	vertex.property(metadata.getDatastoreMetadata().getName()).remove();
    }

    @Override
    public Object getProperty(DuctileVertex vertex, PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> metadata) {
	VertexProperty<Object> property = vertex.property(metadata.getDatastoreMetadata().getName());
	if (!property.isPresent()) {
	    return null;
	}
	return property.value();
    }

    @Override
    public boolean isEntity(Object o) {
	return DuctileDBVertex.class.isAssignableFrom(o.getClass());
    }

    @Override
    public Set<String> getEntityDiscriminators(DuctileVertex vertex) {
	Set<String> discriminators = new HashSet<>();
	for (String label : vertex.getBaseVertex().getTypes()) {
	    discriminators.add(label);
	}
	if (discriminators.size() == 0) {
	    throw new XOException(
		    "A vertex was found without discriminators. Does another framework alter the database?");
	}
	return discriminators;
    }

    @Override
    public Long getEntityId(DuctileVertex vertex) {
	return vertex.id();
    }

    @Override
    public DuctileVertex createEntity(TypeMetadataSet<EntityTypeMetadata<DuctileVertexMetadata>> types,
	    Set<String> discriminators,
	    Map<PrimitivePropertyMethodMetadata<DuctilePropertyMetadata>, Object> exampleEntity) {
	DuctileVertex vertex = graph.addVertex();
	for (String discriminator : discriminators) {
	    vertex.getBaseVertex().addType(discriminator);
	}
	return vertex;
    }

    @Override
    public void deleteEntity(DuctileVertex vertex) {
	vertex.remove();
    }

    @Override
    public ResultIterator<DuctileVertex> findEntity(EntityTypeMetadata<DuctileVertexMetadata> type,
	    String discriminator, Map<PrimitivePropertyMethodMetadata<DuctilePropertyMetadata>, Object> values) {
	if (values.size() > 1) {
	    throw new XOException("Only one property value is supported for find operation");
	}

	IndexedPropertyMethodMetadata<?> indexedProperty = type.getDatastoreMetadata().getIndexedProperty();
	if (indexedProperty == null) {
	    indexedProperty = type.getIndexedProperty();
	}
	if (indexedProperty == null) {
	    throw new XOException(
		    "Type " + type.getAnnotatedType().getAnnotatedElement().getName() + " has no indexed property.");
	}
	PrimitivePropertyMethodMetadata<DuctilePropertyMetadata> propertyMethodMetadata = indexedProperty
		.getPropertyMethodMetadata();
	String name = propertyMethodMetadata.getDatastoreMetadata().getName();
	Object value = values.values().iterator().next();
	Iterator<Vertex> vertices = graph.traversal().V().addV(discriminator).property(name, value);
	List<DuctileVertex> result = new ArrayList<>();
	while (vertices.hasNext()) {
	    result.add((DuctileVertex) vertices.next());
	}
	final Iterator<DuctileVertex> iterator = result.iterator();
	return new ResultIterator<DuctileVertex>() {

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public DuctileVertex next() {
		return iterator.next();
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
    public DuctileVertex findEntityById(EntityTypeMetadata<DuctileVertexMetadata> metadata, String discriminator,
	    Long id) {
	Iterator<Vertex> vertices = graph.vertices(id);
	if (!vertices.hasNext()) {
	    return null;
	}
	return (DuctileVertex) vertices.next();
    }

    @Override
    public void migrateEntity(DuctileVertex vertex, TypeMetadataSet<EntityTypeMetadata<DuctileVertexMetadata>> types,
	    Set<String> discriminators, TypeMetadataSet<EntityTypeMetadata<DuctileVertexMetadata>> targetTypes,
	    Set<String> targetDiscriminators) {
	for (String discriminator : discriminators) {
	    if (!targetDiscriminators.contains(discriminator)) {
		vertex.property(discriminator).remove();
	    }
	}
	for (String discriminator : targetDiscriminators) {
	    if (!discriminators.contains(discriminator)) {
		vertex.getBaseVertex().addType(discriminator);
	    }
	}
    }

    @Override
    public void flushEntity(DuctileVertex vertex) {
	// intentionally left empty
    }

    @Override
    public void addDiscriminators(DuctileVertex entity, Set<String> discriminators) {
	for (String dicriminator : discriminators) {
	    entity.getBaseVertex().addType(dicriminator);
	}
    }

    @Override
    public void removeDiscriminators(DuctileVertex entity, Set<String> discriminators) {
	for (String dicriminator : discriminators) {
	    entity.getBaseVertex().removeType(dicriminator);
	}
    }

}
