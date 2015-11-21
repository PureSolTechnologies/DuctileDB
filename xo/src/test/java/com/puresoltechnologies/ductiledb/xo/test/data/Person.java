package com.puresoltechnologies.ductiledb.xo.test.data;

import java.util.Set;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@VertexDefinition
public interface Person {

	void setFirstName(String firstName);

	String getFirstName();

	void setLastName(String lastName);

	@Indexed
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
