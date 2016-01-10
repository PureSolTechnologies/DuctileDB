package com.puresoltechnologies.ductiledb.xo.test.mapping;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("F")
public interface F {

    E2F getE2F();

    void setValue(String value);

    String getValue();

}
