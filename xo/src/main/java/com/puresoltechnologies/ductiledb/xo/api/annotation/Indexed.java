package com.puresoltechnologies.ductiledb.xo.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.buschmais.xo.spi.annotation.IndexDefinition;

/**
 * <p>
 * Marks a property as indexed.
 * </p>
 * <p>
 * An indexed property is used to find instances using XOManager.
 * </p>
 */
@IndexDefinition
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Indexed {

    boolean unique() default false;

}
