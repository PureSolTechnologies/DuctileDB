package com.puresoltechnologies.ductiledb.xo.test.filesystem;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;

@VertexDefinition("file")
public interface File {

    @Indexed
    String getName();

    void setName(String name);

    @Incoming
    @ContainsFile
    Directory getParentDirectory();

}
