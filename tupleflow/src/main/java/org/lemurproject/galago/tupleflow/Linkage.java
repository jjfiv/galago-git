// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 *
 * @author trevor
 */
public class Linkage {

  public static void link(Step source, Step stage, String fieldName) throws IncompatibleProcessorException {
    try {
      Class sourceClass = source.getClass();
      Field processorField = sourceClass.getField(fieldName);
      Class fieldClass = processorField.getType();

      // are they directly compatible?  if so, finish quickly
      if (fieldClass.isInstance(stage)) {
        processorField.set(source, stage);
        return;
      }

      // try to determine the kind of thing that the stage is
      Class stageClass = stage.getClass();

      for (Class sourceInterfaceClass : sourceClass.getInterfaces()) {
        String interfaceName = sourceInterfaceClass.getName();
        String typeName;
        Object adapted = null;

        try {
          if ((typeName = Utility.strip(interfaceName, "$Source")) != null) {
            // does the stage object have a ShreddedProcessor interface?
            done:
            for (Class stageInterfaceClass : stageClass.getInterfaces()) {
              String stageInterfaceName = stageInterfaceClass.getName();

              if (stageInterfaceName.startsWith(typeName)
                      && stageInterfaceName.endsWith("$ShreddedProcessor")) {
                String stageOrderName = Utility.strip(stageInterfaceName,
                        "$ShreddedProcessor");
                Constructor[] constructors = Class.forName(
                        stageOrderName + "$TupleShredder").getConstructors();

                for (Constructor c : constructors) {
                  Class[] types = c.getParameterTypes();

                  if (types.length == 1 && types[0].isInstance(stage)) {
                    adapted = c.newInstance(stage);
                    break done;
                  }
                }
              }
            }
          } else if ((typeName = Utility.strip(interfaceName, "$ShreddedSource")) != null) {
            Constructor[] constructors = Class.forName(typeName + "$TupleUnshredder").
                    getConstructors();

            for (Constructor c : constructors) {
              Class[] types = c.getParameterTypes();

              if (types.length == 1 && types[0].isInstance(stage)) {
                adapted = c.newInstance(stage);
                break;
              }
            }
          }
        } catch (Exception e) {
          // we're just checking to see if anything works, so exceptions here don't mean anything to us
          continue;
        }

        if (adapted != null && fieldClass.isInstance(adapted)) {
          processorField.set(source, adapted);
          return;
        }
      }

      throw new IncompatibleProcessorException("Stage of type '" + stage.getClass().getName() + "' cannot process the objects that '" + sourceClass.getName() + "' produces.");
    } catch (NoSuchFieldException e) {
      throw new IncompatibleProcessorException(
              "Stage of type '" + source.getClass().getName() + "' has no field called '" + fieldName + "' that can hold a processor object (or maybe it's just not public).");
    } catch (IllegalAccessException e) {
      throw new IncompatibleProcessorException(
              "Stage of type '" + source.getClass().getName() + "' has a field called '" + fieldName + "', but "
              + "it is private or protected and can't be modified by link().");
    }
  }

  public static void link(Step source, Step stage) throws IncompatibleProcessorException {
    link(source, stage, "processor");
  }

  public static void close(Step stage) throws IOException {
    Method close;

    try {
      close = stage.getClass().getMethod("close");
    } catch (NoSuchMethodException e) {
      return;
    }

    try {
      close.invoke(stage);
    } catch (IllegalAccessException e) {
      throw (IOException) new IOException(
              "Couldn't access close method (should declare it public)").initCause(e);
    } catch (InvocationTargetException e) {
      throw (IOException) new IOException("Problem when calling close method").initCause(e);
    }
  }
}
