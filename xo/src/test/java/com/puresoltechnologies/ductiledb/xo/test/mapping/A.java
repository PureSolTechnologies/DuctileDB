package com.puresoltechnologies.ductiledb.xo.test.mapping;

import java.util.List;
import java.util.Set;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Property;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.test.inheritance.Version;

@VertexDefinition("A")
public interface A extends Version {

    @Indexed
    String getIndex();

    void setIndex(String index);

    String getString();

    void setString(String string);

    @Property("MAPPED_STRING")
    String getMappedString();

    void setMappedString(String mapppedString);

    B getB();

    void setB(B b);

    @EdgeDefinition("MAPPED_B")
    B getMappedB();

    void setMappedB(B mappedB);

    Set<B> getSetOfB();

    @EdgeDefinition("MAPPED_SET_OF_B")
    Set<B> getMappedSetOfB();

    List<B> getListOfB();

    @EdgeDefinition("MAPPED_LIST_OF_B")
    List<B> getMappedListOfB();

    Enumeration getEnumeration();

    void setEnumeration(Enumeration enumeration);

    @Property("MAPPED_ENUMERATION")
    Enumeration getMappedEnumeration();

    void setMappedEnumeration(Enumeration enumeration);
}
