package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Element;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.DatastoreMetadataFactory;
import com.buschmais.xo.spi.metadata.method.IndexedPropertyMethodMetadata;
import com.buschmais.xo.spi.metadata.type.TypeMetadata;
import com.buschmais.xo.spi.reflection.AnnotatedElement;
import com.buschmais.xo.spi.reflection.AnnotatedMethod;
import com.buschmais.xo.spi.reflection.AnnotatedType;
import com.buschmais.xo.spi.reflection.PropertyMethod;
import com.puresoltechnologies.ductiledb.api.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileEdge;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Property;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileCollectionPropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileIndexedPropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctilePropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileReferencePropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileVertexMetadata;

/**
 * This class implements the XO DatastoreMetadataFactory for DuctileDB database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileMetadataFactory
	implements DatastoreMetadataFactory<DuctileVertexMetadata, String, DuctileEdgeMetadata, String> {

    @Override
    public DuctileVertexMetadata createEntityMetadata(AnnotatedType annotatedType,
	    Map<Class<?>, TypeMetadata> metadataByType) {
	VertexDefinition annotation = annotatedType.getAnnotation(VertexDefinition.class);
	String value = null;
	IndexedPropertyMethodMetadata<?> indexedProperty = null;
	if (annotation != null) {
	    value = annotation.value();
	    if ((value == null) || (value.isEmpty())) {
		value = annotatedType.getName();
	    }
	    Class<?> usingIndexOf = annotation.usingIndexedPropertyOf();
	    if (!Object.class.equals(usingIndexOf)) {
		TypeMetadata typeMetadata = metadataByType.get(usingIndexOf);
		indexedProperty = typeMetadata.getIndexedProperty();
	    }
	}
	return new DuctileVertexMetadata(value, indexedProperty);
    }

    @Override
    public <ImplementedByMetadata> ImplementedByMetadata createImplementedByMetadata(AnnotatedMethod annotatedMethod) {
	return null;
    }

    @Override
    public DuctileCollectionPropertyMetadata createCollectionPropertyMetadata(PropertyMethod propertyMethod) {
	String name = determinePropertyName(propertyMethod);
	EdgeDirection direction = determineEdgeDirection(propertyMethod);
	return new DuctileCollectionPropertyMetadata(name, direction);
    }

    @Override
    public DuctileReferencePropertyMetadata createReferencePropertyMetadata(PropertyMethod propertyMethod) {
	String name = determinePropertyName(propertyMethod);
	EdgeDirection direction = determineEdgeDirection(propertyMethod);
	return new DuctileReferencePropertyMetadata(name, direction);
    }

    @Override
    public DuctilePropertyMetadata createPropertyMetadata(PropertyMethod propertyMethod) {
	Property property = propertyMethod.getAnnotationOfProperty(Property.class);
	String name = property != null ? property.value() : propertyMethod.getName();
	return new DuctilePropertyMetadata(name);
    }

    /**
     * This method is a helper method to extract the name from a
     * {@link PropertyMethod}.
     * 
     * @param propertyMethod
     *            is the {@link PropertyMethod} object which represents the
     *            method for which the name is to be checked.
     * @return A {@link String} object is returned containing the name of the
     *         edge.
     */
    private static String determinePropertyName(PropertyMethod propertyMethod) {
	VertexDefinition property = propertyMethod.getAnnotationOfProperty(VertexDefinition.class);
	return property != null ? property.value() : propertyMethod.getName();
    }

    /**
     * This method is a helper method to extract the edge direction from a
     * {@link PropertyMethod}.
     * 
     * @param propertyMethod
     *            is the {@link PropertyMethod} object which represents the
     *            method for which the edge direction is to be checked.
     * @return A {@link EdgeDirection} object is returned containing the
     *         direction of the edge.
     */
    private static EdgeDirection determineEdgeDirection(PropertyMethod propertyMethod) {
	Outgoing outgoingAnnotation = propertyMethod.getAnnotation(Outgoing.class);
	Incoming incomingAnnotation = propertyMethod.getAnnotation(Incoming.class);
	if ((outgoingAnnotation != null) && (incomingAnnotation != null)) {
	    return EdgeDirection.BOTH;
	} else if (incomingAnnotation != null) {
	    return EdgeDirection.IN;
	} else {
	    return EdgeDirection.OUT;
	}
    }

    @Override
    public DuctileIndexedPropertyMetadata createIndexedPropertyMetadata(PropertyMethod propertyMethod) {
	Property property = propertyMethod.getAnnotationOfProperty(Property.class);
	String name = property != null ? property.value() : propertyMethod.getName();
	Class<?> declaringClass = propertyMethod.getAnnotatedElement().getDeclaringClass();
	Class<? extends Element> type = null;
	if (declaringClass.getAnnotation(VertexDefinition.class) != null) {
	    type = DuctileVertex.class;
	} else if (declaringClass.getAnnotation(EdgeDefinition.class) != null) {
	    type = DuctileEdge.class;
	} else {
	    throw new XOException("Property '" + name
		    + "' was found with index annotation, but the declaring type is neither a vertex nor an edge.");
	}
	Indexed indexedAnnotation = propertyMethod.getAnnotation(Indexed.class);
	boolean unique = indexedAnnotation.unique();
	Class<?> dataType = propertyMethod.getType();
	if (Serializable.class.isAssignableFrom(dataType)) {
	    @SuppressWarnings("unchecked")
	    Class<? extends Serializable> serializableDataType = (Class<? extends Serializable>) dataType;
	    return new DuctileIndexedPropertyMetadata(name, unique, serializableDataType, type);
	} else {
	    throw new XOException("Illegal data type '" + dataType.getName() + "' found. Type is not serializable.");
	}
    }

    @Override
    public DuctileEdgeMetadata createRelationMetadata(AnnotatedElement<?> annotatedElement,
	    Map<Class<?>, TypeMetadata> metadataByType) {
	EdgeDefinition relationAnnotation;
	if (annotatedElement instanceof PropertyMethod) {
	    relationAnnotation = ((PropertyMethod) annotatedElement).getAnnotationOfProperty(EdgeDefinition.class);
	} else {
	    relationAnnotation = annotatedElement.getAnnotation(EdgeDefinition.class);
	}
	String name = null;
	if (relationAnnotation != null) {
	    String value = relationAnnotation.value();
	    if (!EdgeDefinition.DEFAULT_VALUE.equals(value)) {
		name = value;
	    }
	}
	if (name == null) {
	    name = StringUtils.uncapitalize(annotatedElement.getName());
	}
	return new DuctileEdgeMetadata(name);
    }
}
