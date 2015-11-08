package com.puresoltechnologies.xo.titan.test.label;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("EXPLICIT_LABEL")
public interface ExplicitLabel {

	String getString();

	void setString(String string);

}
