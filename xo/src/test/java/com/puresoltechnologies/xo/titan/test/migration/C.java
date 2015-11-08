package com.puresoltechnologies.xo.titan.test.migration;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("C")
public interface C {

	String getName();

	void setName(String name);

}
