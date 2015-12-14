package com.puresoltechnologies.ductiledb.xo.impl.metadata;

import com.buschmais.xo.spi.datastore.DatastoreEntityMetadata;
import com.buschmais.xo.spi.metadata.method.IndexedPropertyMethodMetadata;

public class DuctileVertexMetadata implements DatastoreEntityMetadata<String> {

	private final String discriminator;
	private final IndexedPropertyMethodMetadata<?> indexedProperty;

	public DuctileVertexMetadata(String discriminator,
			IndexedPropertyMethodMetadata<?> indexedProperty) {
		super();
		this.discriminator = discriminator;
		this.indexedProperty = indexedProperty;
	}

	@Override
	public String getDiscriminator() {
		return discriminator;
	}

	public IndexedPropertyMethodMetadata<?> getIndexedProperty() {
		return indexedProperty;
	}

}
