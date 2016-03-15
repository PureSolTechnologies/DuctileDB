package com.puresoltechnologies.ductiledb.xo.test.withSchema;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("A")
public interface A {

    @Indexed
    String getName();

    void setName(String name);

    String getNeededProperty();

    void setNeededProperty(String value);
}
