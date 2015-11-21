package com.puresoltechnologies.ductiledb.xo.test.relation.qualified;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;

@EdgeDefinition("OneToOne")
@Retention(RUNTIME)
public @interface QualifiedOneToOne {
}
