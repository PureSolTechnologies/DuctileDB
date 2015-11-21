package com.puresoltechnologies.ductiledb.xo.test.label;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition
public interface ImplicitLabel {

	String getString();

	void setString(String string);

}
