package com.puresoltechnologies.xo.titan.test.filesystem;

import java.util.List;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@VertexDefinition("directory")
public interface Directory {

    @Indexed
    String getName();

    void setName(String name);

    @Incoming
    @ContainsDirectory
    Directory getParentDirectory();

    @Outgoing
    @ContainsFile
    List<File> getFiles();

    @Outgoing
    @ContainsDirectory
    List<Directory> getDirectories();

}
