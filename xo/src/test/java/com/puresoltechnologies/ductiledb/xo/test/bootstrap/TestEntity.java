package com.puresoltechnologies.ductiledb.xo.test.bootstrap;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition
public interface TestEntity {

	@Indexed
	String getName();

	void setName(String name);

	TestEntity getTestEntity();

	void setTestEntity(TestEntity testEntity);

}
