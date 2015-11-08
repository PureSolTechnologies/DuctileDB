package com.puresoltechnologies.xo.titan.test.relation.typed;

import java.util.List;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("A")
public interface A {

	TypedOneToOneRelation getOneToOne();

	List<TypedOneToManyRelation> getOneToMany();

	List<TypedManyToManyRelation> getManyToMany();

}
