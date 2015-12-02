package com.puresoltechnologies.ductiledb.xo.test.data;

import java.util.Set;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;
import com.puresoltechnologies.ductiledb.core.StarWarsGraph;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Property;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition
public interface Person {

    void setFirstName(String firstName);

    String getFirstName();

    void setLastName(String lastName);

    @Indexed
    @Property(StarWarsGraph.LAST_NAME_PROPERTY)
    String getLastName();

    void setMother(Person mother);

    @Outgoing
    Person getMother();

    void setFather(Person father);

    @Outgoing
    Person getFather();

    @Outgoing
    Set<Person> getSisters();

    @Outgoing
    Set<Person> getBrothers();
}
