package com.puresoltechnologies.ductiledb.xo.test.relation.qualified;

import java.util.List;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;

@VertexDefinition
public interface B {

	@Incoming
	@QualifiedOneToOne
	A getOneToOne();

	void setOneToOne(A a);

	@Incoming
	@QualifiedOneToMany
	A getManyToOne();

	@Incoming
	@QualifiedManyToMany
	List<A> getManyToMany();

}
