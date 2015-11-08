package com.puresoltechnologies.xo.titan.test.relation.qualified;

import java.util.List;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@VertexDefinition
public interface A {

	@Outgoing
	@QualifiedOneToOne
	B getOneToOne();

	void setOneToOne(B b);

	@Outgoing
	@QualifiedOneToMany
	List<B> getOneToMany();

	@Outgoing
	@QualifiedManyToMany
	List<B> getManyToMany();

}
