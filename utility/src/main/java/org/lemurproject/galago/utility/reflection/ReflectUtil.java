package org.lemurproject.galago.utility.reflection;

import java.lang.reflect.Constructor;

/**
 * Utilities for using reflection that avoid warnings and large number of catch-clauses.
 * @author jfoley
 */
public class ReflectUtil {
  private ReflectUtil() { }

  @SuppressWarnings("unchecked")
  public static <T> Constructor<? extends T> getConstructor(String className, Class<?>... argClasses) throws ReflectiveOperationException {
    return ((Class<? extends T>) Class.forName(className)).getConstructor(argClasses);
  }

  public static <T> T instantiate(Constructor<? extends T> constructor, Object... args) throws ReflectiveOperationException {
    return constructor.newInstance(args);
  }

  @SuppressWarnings("unchecked")
  public static <T> T instantiate(String className) throws ReflectiveOperationException {
    return (T) Class.forName(className).newInstance();
  }
}
