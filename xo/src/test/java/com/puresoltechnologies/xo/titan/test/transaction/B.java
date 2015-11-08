package com.puresoltechnologies.xo.titan.test.transaction;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("B")
public interface B {

    int getIntValue();

    void setIntValue(int intValue);

}
