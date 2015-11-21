package com.puresoltechnologies.ductiledb.xo.test.mapping;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition(value = "D", usingIndexedPropertyOf = A.class)
public interface D extends A {
}
