package com.puresoltechnologies.ductiledb.xo.api.annotation;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.buschmais.xo.spi.annotation.RelationDefinition;
import com.buschmais.xo.spi.annotation.RelationDefinition.FromDefinition;
import com.buschmais.xo.spi.annotation.RelationDefinition.ToDefinition;

/**
 * <p>
 * Defines an edge in Titan.
 * </p>
 * <p>
 * This annotation can be used on the following java elements:
 * </p>
 * <ul>
 * <li>getter methods references or collections of other composite objects
 * (optional).</li>
 * <li>relation qualifier types (mandatory).</li>
 * <li>relation types (mandatory)</li>
 * </ul>
 */
@RelationDefinition
@Retention(RUNTIME)
@Target({ TYPE, ANNOTATION_TYPE, METHOD })
public @interface EdgeDefinition {

    String DEFAULT_VALUE = "";

    /**
     * @return The name of the relation.
     */
    String value() default DEFAULT_VALUE;

    /**
     * Marks a property as incoming relationship.
     */
    @ToDefinition
    @Retention(RUNTIME)
    @Target({ METHOD })
    public @interface Incoming {
    }

    /**
     * Marks a property as outgoing relationship.
     */
    @FromDefinition
    @Retention(RUNTIME)
    @Target({ METHOD })
    public @interface Outgoing {
    }
}
