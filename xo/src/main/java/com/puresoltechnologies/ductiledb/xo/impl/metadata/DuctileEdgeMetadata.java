package com.puresoltechnologies.ductiledb.xo.impl.metadata;

import com.buschmais.xo.spi.datastore.DatastoreRelationMetadata;

public class DuctileEdgeMetadata implements DatastoreRelationMetadata<String> {

	private final String label;

	public DuctileEdgeMetadata(String label) {
		this.label = label;
	}

	@Override
	public String getDiscriminator() {
		return label;
	}
}
