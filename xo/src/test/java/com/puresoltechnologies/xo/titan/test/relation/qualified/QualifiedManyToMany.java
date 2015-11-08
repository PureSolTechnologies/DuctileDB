package com.puresoltechnologies.xo.titan.test.relation.qualified;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;

@EdgeDefinition("ManyToMany")
@Retention(RetentionPolicy.RUNTIME)
public @interface QualifiedManyToMany {
}
