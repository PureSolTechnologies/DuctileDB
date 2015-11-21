package com.puresoltechnologies.ductiledb.xo.test.migration;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("C")
public interface C {

	String getName();

	void setName(String name);

}
