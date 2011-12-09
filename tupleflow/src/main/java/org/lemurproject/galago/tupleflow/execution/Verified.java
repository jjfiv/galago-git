// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.execution;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation is applied to Step classes that don't need any extra
 * verification before they're executed.  Classes that don't take
 * any parameters and don't load any readers or writers fall into this
 * category.  Using this annotation tells the verification stage not
 * to issue a warning when it doesn't find a verify class method.
 *
 * @author trevor
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Verified {
}
