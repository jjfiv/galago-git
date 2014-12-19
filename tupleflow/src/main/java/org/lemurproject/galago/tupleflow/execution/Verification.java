// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.utility.debug.NullCounter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author trevor
 */
public class Verification {

  private static class VerificationParameters implements TupleFlowParameters {

    StepInformation step;
    Stage stage;

    public VerificationParameters(Stage stage, StepInformation step) {
      this.stage = stage;
      this.step = step;
    }

    @Override
    public Counter getCounter(String name) {
      return NullCounter.instance;
    }

    @Override
    public Processor getTypeWriter(String specification) throws IOException {
      return null;
    }

    @Override
    public TypeReader getTypeReader(String specification) throws IOException {
      return null;
    }

    public boolean writerExists(String specification, String className, String[] order) {
      StageConnectionPoint point = stage.connections.get(specification);

      if (point == null) {
        return false;
      }
      if (point.type != ConnectionPointType.Output) {
        return false;
      }
      if (!className.equals(point.getClassName())) {
        return false;
      }
      if (!compatibleOrders(order, point.getOrder())) {
        return false;
      }
      return true;
    }

    public boolean readerExists(String specification, String className, String[] order) {
      StageConnectionPoint point = stage.connections.get(specification);

      if (point == null) {
        return false;
      }
      if (point.type != ConnectionPointType.Input) {
        return false;
      }
      if (!className.equals(point.getClassName())) {
        return false;
      }
      if (!compatibleOrders(point.getOrder(), order)) {
        return false;
      }
      return true;
    }

    @Override
    public Parameters getJSON() {
      return step.getParameters();
    }

    @Override
    public int getInstanceId() {
      return 0;
    }
  }

  /**
   * Tests to see if two object orders are compatible.  By compatible, we mean that
   * a list of objects in outputOrder is also in inputOrder.  This is true if the orders
   * are identical, but also if inputOrder is more permissive than outputOrder.
   *
   * For instance, suppose we are sorting a list of people's names.  People typically
   * have a surname (last name) and a given name (first name).  In Galago notation,
   * consider these two orders you could use:
   *      +surname
   *      +surname +givenName
   *
   * If a list is ordered by (+surname +givenName), then it is also ordered by
   * +surname.  The reverse isn't true, though: if you order by +surname, you
   * haven't necessarily ordered by (+surname +givenName).  Therefore:
   *      compatibleOrders({ "+surname" }, { "+surname", "+givenName" }) == false
   *      compatibleOrders({ "+surname", "+givenName" }, { "+surname" }) == true
   *
   * @param currentOrder  The current order of the data that is supplied.
   * @param requiredOrder The required order of the data.
   */
  public static boolean compatibleOrders(String[] currentOrder, String[] requiredOrder) {
    // if the required order is more specific than the current order, it's not compatible
    if (currentOrder.length < requiredOrder.length) {
      return false;        // the required order needs to agree with the current order in each case.
    }
    for (int i = 0; i < requiredOrder.length; i++) {
      if (!currentOrder[i].equals(requiredOrder[i])) {
        return false;
      }
    }

    return true;
  }

  public static boolean requireParameters(String[] required, Parameters parameters, ErrorStore store) {
    boolean result = true;
    for (String key : required) {
      if (!parameters.containsKey(key)) {
        store.addError("The parameter '" + key + "' is required.");
        result = false;
      }
    }
    return result;
  }

  public static boolean isOrderAvailable(String typeName, String[] orderSpec) {
    try {
      Class typeClass = Class.forName(typeName);
      Type type = (Type) typeClass.newInstance();
      return type.getOrder(orderSpec) != null;
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isClassAvailable(String name) {
    try {
      Class.forName(name);
    } catch (Exception e) {
      return false;
    }

    return true;
  }

  public static boolean requireOrder(String typeName, String[] orderSpec, ErrorStore store) {
    if (!isOrderAvailable(typeName, orderSpec)) {
      StringBuilder builder = new StringBuilder();

      for (String orderKey : orderSpec) {
        builder.append(orderKey);
      }

      store.addError(
              "The order '" + builder.toString() + "' was not found in " + typeName + ".");
      return false;
    }
    return true;
  }

  public static boolean requireClass(String typeName, ErrorStore store) {
    if (!isClassAvailable(typeName)) {
      store.addError("The class '" + typeName + "' could not be found.");
      return false;
    }
    return true;
  }

  public static boolean requireWriteableFile(String pathname, ErrorStore store) {
    File path = new File(pathname);

    if (path.exists() && !path.isFile()) {
      store.addError("Pathname " + pathname + " exists already and isn't a file.");
      return false;
    }

    return requireWriteableDirectoryParent(pathname, store);

  }

  public static boolean requireWriteableDirectory(String pathname, ErrorStore store) {
    File path = new File(pathname);

    if (path.isFile()) {
      store.addError("Pathname " + pathname + " is a file, but a directory is required.");
      return false;
    }

    if (path.isDirectory() && !path.canWrite()) {
      store.addError("Pathname " + pathname + " is a directory, but it isn't writable.");
      return false;
    }

    return requireWriteableDirectoryParent(pathname, store);
  }

  /**
   * <p>If pathname exists, returns true.  If pathname doesn't exist, checks to
   * see if it's possible for this process to create something called pathname.</p>
   *
   * <p>This method returns false if the closest existing parent directory of pathname
   * is not writeable (or isn't a directory)</p>
   *
   * <p>For example, if filename is /a/b/c/d/e/f, this method will return true if:
   * <ul>
   * <li>/a/b/c/d/e/f exists</li>
   * <li>/a/b/c/d/e/f doesn't exist, but /a/b/c/d/e does, and is writeable</li>
   * <li>/a/b/d/d/e doesn't exist, but /a/b/c/d does, and is writeable</li>
   * <li>/a doesn't exist, but / does, and is writeable.</li>
   * </ul>
   * </p>
   */
  public static boolean requireWriteableDirectoryParent(final String pathname, final ErrorStore store) {
    File path = new File(pathname);

    if (!path.exists()) {
      String parent = path.getParent();

      while (parent != null && !new File(parent).exists()) {
        parent = new File(parent).getParent();
      }

      if (parent == null) {
        parent = System.getProperty("user.dir");
      }

      if (!new File(parent).canWrite()) {
        store.addError(
                "Pathname " + pathname + " doesn't exist, and the parent directory isn't writable.");
        return false;
      }
    }

    return true;
  }

  private static class TypeState {

    public String className;
    public String[] order;
    public boolean defined;

    public TypeState() {
      this.className = "java.lang.Object";
      this.order = new String[0];
      this.defined = false;
    }

    public TypeState(TypeState state) {
      this.className = state.getClassName();
      this.order = state.getOrder();
      this.defined = state.isDefined();
    }

    public boolean check(String className, String[] order) {
      if (!defined) {
        return true;
      }
      return className.equals(this.className) && Verification.compatibleOrders(order,
              this.order);
    }

    public String[] getOrder() {
      return order;
    }

    public String getClassName() {
      return className;
    }

    public void update(String className, String[] order) {
      this.className = className;
      this.order = order;
      this.defined = true;
    }

    public void setDefined(boolean defined) {
      this.defined = defined;
    }

    private boolean isDefined() {
      return defined;
    }
  }

  public static void verify(TypeState state, Stage stage, List<StepInformation> steps, ErrorStore store) {
    for (int i = 0; i < steps.size(); i++) {
      StepInformation step = steps.get(i);
      boolean isLastStep = (i == (steps.size() - 1));

      if (step instanceof InputStepInformation) {
        // This step was an <input> tag
        InputStepInformation input = (InputStepInformation) step;
        StageConnectionPoint point = stage.connections.get(input.getId());

        if (point == null) {
          store.addError(step.getLocation(),
                  "Input references a connection called '"
                  + input.getId() + "', but it isn't listed in the connections section of the stage.");
        } else {
          state.update(point.getClassName(), point.getOrder());
        }
      } else if (step instanceof MultiInputStepInformation) {
        // This step was a <multiinput> tag

        String[] points = ((MultiInputStepInformation) step).getIds();
        String className = null;
        String[] order = null;
        StageConnectionPoint actual = stage.connections.get(points[0]);
        boolean erred = false;
        if (actual == null) {
          store.addError(step.getLocation(),
                  "Input references a connection called '"
                  + points[0] + "', but it isn't listed in the connections section of the stage.");
          erred = true;
        } else {
          className = actual.getClassName();
          order = actual.getOrder();
          for (int ip = 1; ip < points.length; ip++) {
            actual = stage.connections.get(points[ip]);
            if (actual == null) {
              store.addError(step.getLocation(),
                      "MultiInput connection '" + points[ip] + "' isn't listed in the connections section of the stage.");
              erred = true;
            } else if (!actual.getClassName().equals(className)) {
              store.addError(step.getLocation(),
                      "MultiInput connection '" + points[ip] + "' uses a different input type than '" + className + "'");
              erred = true;
            } else if (!equals(order, actual.getOrder())) {
              store.addError(step.getLocation(),
                      "MultiInput connection '" + points[ip] + "', uses order " + Arrays.toString(actual.getOrder()) + ", but primary"
                      + "input uses order " + Arrays.toString(order));
              erred = true;
            }
          }
        }

        if (!erred) {
          state.update(className, order);
        }
      } else if (step instanceof OutputStepInformation) {
        // This step was an <output> tag
        OutputStepInformation output = (OutputStepInformation) step;
        StageConnectionPoint point = stage.connections.get(output.getId());

        if (point == null) {
          store.addError(step.getLocation(),
                  "Output references a connection called '"
                  + output.getId() + "', but it isn't listed in the connections section of the stage.");
        } else {
          if (state.isDefined() && !state.getClassName().equals(point.getClassName())) {
            store.addError(step.getLocation(), "Previous (" + step.getClassName() + ") step makes '"
                    + state.getClassName() + "' objects, but this output connection wants '"
                    + point.getClassName() + "' objects.");
          } else if (state.isDefined() && !compatibleOrders(state.getOrder(), point.getOrder())) {
            store.addError(step.getLocation(), "Previous step (" + step.getClassName() + ") outputs objects in '"
                    + Arrays.toString(state.getOrder()) + "' order, but incompatible order '"
                    + Arrays.toString(point.getOrder()) + "' is required.");
          }
        }

        state.setDefined(false);
      } else if (step instanceof MultiStepInformation) {
        // This is a <multi> tag.  The MultiStep object contains
        // many different object groups.
        MultiStepInformation multiStep = (MultiStepInformation) step;

	for (String groupName : multiStep) {
	    verify(new TypeState(state), stage, multiStep.getGroup(groupName), store);
          state.setDefined(false);
        }
      } else {
        Class clazz;
        try {
          clazz = Class.forName(step.getClassName());
        } catch (ClassNotFoundException ex) {
          store.addError(step.getLocation(), "Couldn't find class: " + step.getClassName());
          continue;
        }

        VerificationParameters vp = new VerificationParameters(stage, step);

        verifyInputClass(state, step, clazz, vp, store);
        verifyStepClass(clazz, step, store, vp);

        if (!isLastStep) {
          verifyOutputClass(state, clazz, step, store, vp);
        }
      }
    }
  }

  private static void verifyOutputClass(TypeState state, final Class clazz, final StepInformation step, final ErrorStore store, final VerificationParameters vp) {
    String[] outputOrder = new String[0];
    String outputClass = "java.lang.Object";

    try {
      OutputClass outputClassAnnotation = (OutputClass) clazz.getAnnotation(OutputClass.class);

      if (outputClassAnnotation != null) {
        outputClass = outputClassAnnotation.className();
        outputOrder = outputClassAnnotation.order();
        state.update(outputClass, outputOrder);

        if (!Verification.isClassAvailable(outputClass)) {
          store.addError(step.getLocation(), step.getClassName() + ": Class " + step.getClassName() + " has an "
                  + "@OutputClass annotation with the class name '" + outputClass
                  + "' which couldn't be found.");
          state.setDefined(false);
        } else {
          state.update(outputClass, outputOrder);
        }
      } else {
        try {
          Method getOutputClass = clazz.getMethod("getOutputClass",
                  TupleFlowParameters.class);

          if (getOutputClass.getReturnType() == String.class) {
            outputClass = (String) getOutputClass.invoke(null, vp);
            outputOrder = new String[0];

            try {
              Method getOutputOrder = clazz.getMethod("getOutputOrder",
                      TupleFlowParameters.class);
              outputOrder = (String[]) getOutputOrder.invoke(null, vp);
            } catch (NoSuchMethodException e) {
              // ignore this one
            }

            if (!Verification.isClassAvailable(outputClass)) {
              store.addError(step.getLocation(),
                      step.getClassName() + ": Class " + step.getClassName() + " returned "
                      + "an output class name '" + outputClass + "' which couldn't be found.");
              state.setDefined(false);
            } else {
              state.update(outputClass, outputOrder);
            }
          } else {
            store.addError(step.getLocation(), step.getClassName() + " has a class method called getOutputClass, "
                    + "but it returns something other than java.lang.String.");
            state.setDefined(false);
          }
        } catch (NoSuchMethodException e) {
          store.addWarning(step.getLocation(), step.getClassName() + ": Class " + step.getClassName() + " has no suitable "
                  + "getOutputClass method and no @OutputClass annotation.");
          state.setDefined(false);
        }
      }
    } catch (InvocationTargetException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an InvocationTargetException while verifying class: " + e.getMessage());
      state.setDefined(false);
    } catch (SecurityException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught a SecurityException while verifying class: " + e.getMessage());
      state.setDefined(false);
    } catch (IllegalArgumentException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an IllegalArgumentException while verifying class: " + e.getMessage());
      state.setDefined(false);
    } catch (IllegalAccessException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an IllegalAccessException while verifying class: " + e.getMessage());
      state.setDefined(false);
    }
  }

  private static void verifyStepClass(final Class clazz, final StepInformation step, final ErrorStore store, final VerificationParameters vp) {
    try {
      Verified verifiedAnnotation = (Verified) clazz.getAnnotation(Verified.class);

      // if this class is already verified, we can move on
      if (verifiedAnnotation != null) {
        return;
      }
      Method verify = clazz.getMethod("verify", TupleFlowParameters.class,
              ErrorStore.class);

      if (verify == null) {
        store.addWarning(step.getLocation(), "Class " + step.getClassName()
                + " has no suitable verify method.");
      } else if (Modifier.isStatic(verify.getModifiers()) == false) {
       store.addWarning(step.getLocation(), "Class " + step.getClassName()
                + " has a verify method, but it isn't static.");
      } else {
        verify.invoke(null, vp, store);
      }
    } catch (InvocationTargetException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an InvocationTargetException while verifying step class: " + e.getMessage());
    } catch (SecurityException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught a SecurityException while verifying step class: " + e.getMessage());
    } catch (NoSuchMethodException e) {
      store.addWarning(step.getLocation(),
              "Class " + step.getClassName() + " has no suitable verify method.");
    } catch (IllegalArgumentException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an IllegalArgumentException while verifying class: " + e.getMessage());
    } catch (IllegalAccessException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an IllegalAccessException while verifying class: " + e.getMessage());
    }
  }

  private static Class findInputClassType(Class clazz) {
    Method[] allMethods = clazz.getMethods();

    for (Method method : allMethods) {
      if (!method.getName().equals("process")) {
        continue;
      }
      Class[] types = method.getParameterTypes();
      if (types.length != 1) {
        continue;
      }
      if (types[0] == Object.class) {
        continue;
      }
      return types[0];
    }
    return null;
  }

  private static void verifyInputClass(TypeState state, final StepInformation step, final Class clazz, final VerificationParameters vp, final ErrorStore store) {
    if (!state.isDefined()) {
      return;
    }
    try {
      Class inputClass = findInputClassType(clazz);

      InputClass inputClassAnnotation = (InputClass) clazz.getAnnotation(InputClass.class);
      String inputClassName = "unknown";
      String[] inputOrder = new String[0];

      if (inputClassAnnotation != null) {
        inputClassName = inputClassAnnotation.className();
        inputOrder = inputClassAnnotation.order();

        if (inputClass != null && !inputClassName.equals(inputClass.getName())) {
          String outputMessage = String.format("%s: Class %s has an @InputClass "
                  + "annotation with the class name '%s', but the process() method takes "
                  + "'%s' objects.", step.getClassName(), step.getClassName(),
                  inputClassName, inputClass.getName());
          store.addError(step.getLocation(), outputMessage);
        }

        if (!Verification.isClassAvailable(inputClassName)) {
          store.addError(step.getLocation(), step.getClassName() + ": Class " + step.getClassName() + " has an "
                  + "@InputClass annotation with the class name '" + inputClassName
                  + "' which couldn't be found.");
        }
      } else {
        try {
          Method getInputClass = clazz.getMethod("getInputClass",
                  TupleFlowParameters.class);

          if (getInputClass.getReturnType() == String.class) {
            inputClassName = (String) getInputClass.invoke(null, vp);

            try {
              Method getInputOrder = clazz.getMethod("getInputOrder",
                      TupleFlowParameters.class);
              inputOrder = (String[]) getInputOrder.invoke(null, vp);
            } catch (NoSuchMethodException e) {
              // ignore this one
            }

            if (!Verification.isClassAvailable(inputClassName)) {
              store.addError(step.getLocation(), step.getClassName() + ": Class " + step.getClassName() + " has an "
                      + "returned '" + inputClassName + "' from getInputClass, but "
                      + "it couldn't be found.");
            }
          } else {
            store.addError(step.getLocation(), step.getClassName() + " has a class method called getInputClass, "
                    + "but it returns something other than java.lang.String.");
          }
        } catch (NoSuchMethodException e) {
          store.addWarning(step.getLocation(), step.getClassName() + ": Class " + step.getClassName() + " has no suitable "
                  + "getInputClass method and has no @InputClass annotation.");
          return;
        }
      }

      if (state.isDefined()) {
        if (!inputClassName.equals(state.getClassName())) {
          String err = "Current pipeline class '" + state.getClassName()
                  + "' is different than the required type: '"
                  + inputClassName + "'.";
          store.addError(step.getLocation(), err);
          throw new RuntimeException(String.format("%s\n%s\n\n", step.getClassName(), err));
        }

        if (!compatibleOrders(state.getOrder(), inputOrder)) {
          store.addError(step.getLocation(),
                  "Current object order '" + Arrays.toString(state.getOrder()) + "' is incompatible "
                  + "with the required input order: '" + Arrays.toString(inputOrder) + "'.");
        }
      }
    } catch (InvocationTargetException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an InvocationTargetException while verifying class: " + e.getMessage());
    } catch (SecurityException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught a SecurityException while verifying class: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an IllegalArgumentException while verifying class: " + e.getMessage());
    } catch (IllegalAccessException e) {
      store.addError(step.getLocation(),
              step.getClassName() + ": Caught an IllegalAccessException while verifying class: " + e.getMessage());
    }
  }

  public static void verify(Stage stage, ErrorStore store) {
    TypeState state = new TypeState();
    verify(state, stage, stage.steps, store);
  }

  public static void verify(Job job, ErrorStore store) {
    for (Stage stage : job.stages.values()) {
      verify(stage, store);
    }
  }

  public static boolean verifyTypeReader(String readerName, Class typeClass, TupleFlowParameters parameters, ErrorStore store) {
    return verifyTypeReader(readerName, typeClass, new String[0], parameters, store);
  }

  public static boolean verifyTypeReader(String readerName, Class typeClass, String[] order, TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.readerExists(readerName, typeClass.getName(), order)) {
      store.addError("No reader named '" + readerName + "' was found in this stage.");
      return false;
    }

    return true;
  }

  public static boolean verifyTypeWriter(String readerName, Class typeClass, String order[], TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.writerExists(readerName, typeClass.getName(), order)) {
      store.addError("No writer named '" + readerName + "' was found in this stage.");
      return false;
    }

    return true;
  }

  public static boolean equals(String[] first, String[] second) {
    if (first.length != second.length) {
      return false;
    }

    for (int i = 0; i < first.length; i++) {
      if (!first[i].equals(second[i])) {
        return false;
      }
    }
    return true;
  }
}
