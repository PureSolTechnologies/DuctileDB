package com.puresoltechnologies.ductiledb.xo.test.query;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("A")
public interface A {

    @Indexed
    String getValue();

    void setValue(String value);

    A2B getA2B();

}
