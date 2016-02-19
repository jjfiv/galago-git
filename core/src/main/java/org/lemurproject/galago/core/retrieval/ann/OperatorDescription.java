package org.lemurproject.galago.core.retrieval.ann;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Class annotates galago operators with a string representing format and
 * example of use. 
 *
 * @author smh
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface OperatorDescription {
  String description () default "";
}
