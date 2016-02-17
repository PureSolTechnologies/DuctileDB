package com.puresoltechnologies.ductiledb.xo.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.buschmais.xo.spi.annotation.QueryDefinition;

/**
 * <p>
 * Marks an interface or method as a Gremlin query.
 * </p>
 * <p>
 * For Gremlin language, have a look to:
 * <a href="http://gremlindocs.com">http://gremlindocs.com</a>
 * </p>
 */
@QueryDefinition
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Query {

    /**
     * @return Returns the Gremlin expression as {@link String}.
     */
    String value();

    /**
     * @return A name for the result is returned which needs to reflect a
     *         property type.
     */
    String name() default "";

    /**
     * This method defines which query language is to be used. Enum
     * {@link QueryLanguage} contains all supported languages.
     * 
     * @return The {@link QueryLanguage} is returned.
     */
    QueryLanguage language() default QueryLanguage.GREMLIN;
}
