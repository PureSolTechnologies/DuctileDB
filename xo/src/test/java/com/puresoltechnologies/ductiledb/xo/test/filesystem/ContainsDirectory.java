package com.puresoltechnologies.ductiledb.xo.test.filesystem;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;

@EdgeDefinition("contains_directory")
@Retention(RetentionPolicy.RUNTIME)
public @interface ContainsDirectory {
}
