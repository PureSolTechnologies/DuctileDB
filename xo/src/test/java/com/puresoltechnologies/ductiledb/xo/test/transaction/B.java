package com.puresoltechnologies.ductiledb.xo.test.transaction;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("B")
public interface B {

    int getIntValue();

    void setIntValue(int intValue);

}
