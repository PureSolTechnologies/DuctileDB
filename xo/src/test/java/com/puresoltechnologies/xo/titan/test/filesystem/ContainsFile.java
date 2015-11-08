package com.puresoltechnologies.xo.titan.test.filesystem;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;

@EdgeDefinition("contains_file")
@Retention(RetentionPolicy.RUNTIME)
public @interface ContainsFile {
}
