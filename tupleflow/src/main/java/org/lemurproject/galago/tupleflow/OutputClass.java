// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 * @author trevor
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface OutputClass {
    String className() default "java.lang.Object";
    String[] order() default {};
}
