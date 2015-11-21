package com.puresoltechnologies.ductiledb.xo.test.label;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("EXPLICIT_LABEL")
public interface ExplicitLabel {

	String getString();

	void setString(String string);

}
