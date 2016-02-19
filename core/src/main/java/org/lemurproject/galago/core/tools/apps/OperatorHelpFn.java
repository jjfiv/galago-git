package org.lemurproject.galago.core.tools.apps;

import org.apache.commons.math3.util.Pair;
import org.lemurproject.galago.core.retrieval.FeatureFactory;
import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.ann.ImplementsOperator;
import org.lemurproject.galago.core.retrieval.ann.OperatorDescription;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.AppFunction;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Dump all the operators registered in FeatureFactory, with as much automatically-collected information as possible.
 * @author jfoley
 */
public class OperatorHelpFn extends AppFunction {
  @Override
  public String getName() {
    return "operator-help";
  }

  @Override
  public String getHelpString() {
    return makeHelpStr(
        "class", "[default=true] Show the classpath of the class that implements a particular operator.",
        "stats", "[default=false] Show information, if any, about the statistics operators require.",
        "parameters", "[default=true] Show information, if any, about the parameters operators require or accept."
    );
  }

  static class ParameterInfo {
    final String name;
    final String type;

    public ParameterInfo(String name) {
      this(name, null);
    }
    public ParameterInfo(String name, String type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String toString() {
      if(type != null) {
        return name+": "+type;
      }
      return name;
    }
  }

  @Override
  public boolean allowsZeroParameters() {
    return true;
  }

  public static void main(String[] args) throws Exception {
    OperatorHelpFn helpfn = new OperatorHelpFn();
    helpfn.run(Parameters.parseArgs(args), System.out);
  }

  public static String typeNameForIteratorClass(Class<?> clazz) {
    List<Pair<Class<? extends BaseIterator>, String>> checks = Arrays.asList(
        Pair.create(ScoreIterator.class, "scores"),
        Pair.create(ExtentIterator.class, "extents"),
        Pair.create(LengthsIterator.class, "lengths"),
        Pair.create(CountIterator.class, "counts"),
        Pair.create(IndicatorIterator.class, "booleans"));

    for (Pair<Class<? extends BaseIterator>, String> check : checks) {
      if(check.getFirst().isAssignableFrom(clazz)) {
        return check.getSecond();
      }
    }
    return "unknown";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    FeatureFactory factory = new FeatureFactory(p);

    boolean showClass = p.get("class", true);
    boolean showStats = p.get("stats", false);
    boolean showParams = p.get("parameters", true);
    boolean showDescriptions = p.get("descriptions", true);
    
    List<String> operatorList = null;

    if (p.containsKey("operator")) {
      //- Just return information on the operators in the operator list
      operatorList = p.getList("operator");
    }
    
    final Map<Class<?>, List<String>> operatorsMap = factory.getOperators();
    
    //factory.getOperators().forEach((clazz, names) -> {
    operatorsMap.forEach ((clazz, names) -> {
      
    //  if (operatorList != null && operatorList.size() > 0) {
    //    names.retainAll (operatorList);
    //  }
      output.println("#"+Utility.join(names, ", #"));

      output.println("\tOutput Type: "+typeNameForIteratorClass(clazz));

      boolean takesNodeParameters = false;

      Constructor<?>[] constructors = clazz.getConstructors();
      for (Constructor<?> constructor : constructors) {
        List<String> inputs = new ArrayList<>();
        for (Class<?> input : constructor.getParameterTypes()) {
          if(NodeParameters.class.isAssignableFrom(input)) {
            takesNodeParameters = true;
            continue;
          }
          String kind;
          if(input.isArray()) {
            kind = typeNameForIteratorClass(input.getComponentType())+"[]";
          } else {
            kind = typeNameForIteratorClass(input);
          }
          inputs.add(kind);
        }

        output.println("\tInput Types: "+inputs);

        // since lengths is inserted magically, we also accept operators without lengths automatically specified:
        if(inputs.contains("lengths")) {
          inputs.remove("lengths");
          output.println("\tInput Types: "+inputs);
        }
      }

      if(showClass) {
        output.println("\tDefined by class: " + clazz.getCanonicalName());
      }

      if(takesNodeParameters && showParams) {
        List<ParameterInfo> opParams = new ArrayList<>();
        RequiredParameters reqp = clazz.getAnnotation(RequiredParameters.class);
        if (reqp != null) {
          for (String pname : reqp.parameters()) {
            opParams.add(new ParameterInfo(pname));
          }
        }
        if (!opParams.isEmpty()) {
          output.println("\tUses query parameters: " + opParams);
        }
      }

      if(showStats) {
        RequiredStatistics reqstat = clazz.getAnnotation(RequiredStatistics.class);
        if (reqstat != null) {
          output.println("\tUses statistics: " + Arrays.toString(reqstat.statistics()));
        }
      }

      output.println();

      if (showDescriptions) {
        OperatorDescription opDescript = clazz.getAnnotation (OperatorDescription.class);
        if (opDescript != null) {
          //output.println ("\tOperator Description: " + clazz.getArrays.toString(reqstat.statistics()));
          output.println ("\tDescription: " + opDescript.description ());
        }
      }

      output.println();
    });

    factory.getTraversalNames().forEach(className -> {
      Class<?> clazz = factory.getClassForName(className);

      ImplementsOperator opImpl = clazz.getAnnotation(ImplementsOperator.class);
      if(opImpl == null) return;

      output.println("#"+opImpl.value());
      output.println("\tDefined by class: "+clazz.getCanonicalName());

      if (showDescriptions) {
        OperatorDescription opDescript = clazz.getAnnotation (OperatorDescription.class);
        if (opDescript != null) {
          //output.println ("\tOperator Description: " + clazz.getArrays.toString(reqstat.statistics()));
          output.println ("\tDescription: " + opDescript.description ());
        }
      }

      output.println();
    });
  }
}
