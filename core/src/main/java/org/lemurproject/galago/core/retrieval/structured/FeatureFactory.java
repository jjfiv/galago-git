// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.BadOperatorException;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.traversal.*;
import org.lemurproject.galago.core.retrieval.traversal.optimize.ExtentsToCountLeafTraversal;
import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenCombineTraversal;
import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenWindowTraversal;
import org.lemurproject.galago.core.retrieval.traversal.optimize.MergeCombineChildrenTraversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * @author trevor
 * @author irmarc
 */
public class FeatureFactory {

  static String[][] sOperatorLookup = {
    {ThresholdIterator.class.getName(), "threshold"},
    {ScoreCombinationIterator.class.getName(), "combine"},
    {WeightedSumIterator.class.getName(), "wsum"},
    {SynonymIterator.class.getName(), "syn"},
    {SynonymIterator.class.getName(), "synonym"},
    {ExtentInsideIterator.class.getName(), "inside"},
    {MinimumCountConjunctionIterator.class.getName(), "mincount"},
    {OrderedWindowIterator.class.getName(), "ordered"},
    {OrderedWindowIterator.class.getName(), "od"},
    {OrderedWindowIterator.class.getName(), "quote"}, // don't rely on this - ImplicitFeatureCast does: quote -> od:1 for now. (irmarc)
    {UnorderedWindowIterator.class.getName(), "unordered"},
    {UnorderedWindowIterator.class.getName(), "uw"},
    {UnorderedWindowIterator.class.getName(), "intersect"},
    {ScaleIterator.class.getName(), "scale"},
    {ScoreCombinationIterator.class.getName(), "rm"},
    {ScoreCombinationIterator.class.getName(), "bm25rf"},
    {BM25FCombinationIterator.class.getName(), "bm25fcomb"},
    {BM25FCombinationIterator.class.getName(), "bm25fcomb"},
    {UniversalIndicatorIterator.class.getName(), "all"},
    {ExistentialIndicatorIterator.class.getName(), "any"},
    {NullExtentIterator.class.getName(), "null"},
    {RequireIterator.class.getName(), "require"},
    {RejectIterator.class.getName(), "reject"},
    {GreaterThanIterator.class.getName(), "greater"},
    {LessThanIterator.class.getName(), "less"},
    {InBetweenIterator.class.getName(), "between"},
    {EqualityIterator.class.getName(), "equals"},
    {PassageFilterIterator.class.getName(), "passagefilter"},
    {PL2ScoringIterator.class.getName(), "pl2scorer"},
    {PassageLengthIterator.class.getName(), "passagelengths"},
    {LogProbNotIterator.class.getName(), "logprobnot"}
  };
  static String[][] sFeatureLookup = {
    {DirichletScoringIterator.class.getName(), "dirichlet"},
    {EstimatedDirichletScoringIterator.class.getName(), "dirichlet-est"},
    {JelinekMercerScoringIterator.class.getName(), "linear"},
    {JelinekMercerScoringIterator.class.getName(), "jm"},
    {BM25ScoringIterator.class.getName(), "bm25"},
    {BM25RFScoringIterator.class.getName(), "bm25rf"},
    {BoostingIterator.class.getName(), "boost"},
    {BM25FieldScoringIterator.class.getName(), "bm25f"},
    {LogarithmIterator.class.getName(), "log"},
    {DFRScoringIterator.class.getName(), "dfr"},
    {PL2FieldScoringIterator.class.getName(), "pl2f"},
    {PL2ScoringIterator.class.getName(), "pl2"},
    {InL2ScoringIterator.class.getName(), "inl2"},
    {BiL2ScoringIterator.class.getName(), "bil2"}
  };
  static String[] sTraversalList = {
    RelevanceModelTraversal.class.getName(),
    ReplaceOperatorTraversal.class.getName(),
    StopStructureTraversal.class.getName(),
    StopWordTraversal.class.getName(),
    WeightedSequentialDependenceTraversal.class.getName(),
    SequentialDependenceTraversal.class.getName(),
    FullDependenceTraversal.class.getName(),
    ProximityDFRTraversal.class.getName(),
    PRMS2Traversal.class.getName(),
    TransformRootTraversal.class.getName(),
    WindowRewriteTraversal.class.getName(),
    TextFieldRewriteTraversal.class.getName(),
    InsideToFieldPartTraversal.class.getName(),
    ImplicitFeatureCastTraversal.class.getName(),
    InsertLengthsTraversal.class.getName(),
    PassageRestrictionTraversal.class.getName(),
    ExtentsToCountLeafTraversal.class.getName(),
    FlattenWindowTraversal.class.getName(),
    FlattenCombineTraversal.class.getName(),
    MergeCombineChildrenTraversal.class.getName(),
    AnnotateParameters.class.getName(),
    AnnotateCollectionStatistics.class.getName(),
    DeltaCheckTraversal.class.getName()
  };

  public FeatureFactory(Parameters p) {
    this(p, sOperatorLookup, sFeatureLookup, sTraversalList);
  }

  public FeatureFactory(Parameters parameters,
          String[][] sOperatorLookup, String[][] sFeatureLookup,
          String[] sTraversalList) {
    operatorLookup = new HashMap<String, OperatorSpec>();
    featureLookup = new HashMap<String, OperatorSpec>();
    this.parameters = parameters;

    for (String[] item : sFeatureLookup) {
      OperatorSpec operator = new OperatorSpec();
      operator.className = item[0];
      String operatorName = item[1];
      featureLookup.put(operatorName, operator);
    }

    for (String[] item : sOperatorLookup) {
      OperatorSpec operator = new OperatorSpec();
      operator.className = item[0];
      String operatorName = item[1];
      operatorLookup.put(operatorName, operator);
    }

    ArrayList<TraversalSpec> afterTraversals = new ArrayList<TraversalSpec>();
    ArrayList<TraversalSpec> beforeTraversals = new ArrayList<TraversalSpec>();
    ArrayList<TraversalSpec> insteadTraversals = new ArrayList<TraversalSpec>();

    if (parameters.isMap("traversals") || parameters.isList("traversals", Parameters.Type.MAP)) {
      List<Parameters> traversals = (List<Parameters>) parameters.getAsList("traversals");
      for (Parameters traversal : traversals) {
        String className = traversal.getString("name");
        String order = traversal.get("order", "after");

        TraversalSpec spec = new TraversalSpec();
        spec.className = className;

        if (order.equals("before")) {
          beforeTraversals.add(spec);
        } else if (order.equals("after")) {
          afterTraversals.add(spec);
        } else if (order.equals("instead")) {
          insteadTraversals.add(spec);
        } else {
          throw new RuntimeException("order must be one of {before,after,instead}");
        }
      }
    }

    // If the user doesn't want to replace the current pipeline, add in that pipeline
    if (insteadTraversals.size() == 0) {
      for (String className : sTraversalList) {
        TraversalSpec spec = new TraversalSpec();
        spec.className = className;
        insteadTraversals.add(spec);
      }
    }


    traversals = new ArrayList<TraversalSpec>();
    traversals.addAll(beforeTraversals);
    traversals.addAll(insteadTraversals);
    traversals.addAll(afterTraversals);

    // Load external operators
    if (parameters.containsKey("operators")) {
      Parameters operators = parameters.getMap("operators");
      for (String operatorName : operators.getKeys()) {
        String className = operators.getString(operatorName);
        OperatorSpec spec = new OperatorSpec();
        spec.className = className;
        operatorLookup.put(operatorName, spec);
      }
    }

    // Load external features
    if (parameters.isList("features")) {
      for (Parameters value : (List<Parameters>) parameters.getList("features")) {
        String className = value.getString("class");
        String operatorName = value.getString("name");
        Parameters params = value.isMap("parameters") ? value.getMap("parameters") : null;
        OperatorSpec spec = new OperatorSpec();
        spec.className = className;
        featureLookup.put(operatorName, spec);
      }
    }
  }

  public static class OperatorSpec {

    public String className;
  }

  public static class TraversalSpec {

    public String className;
  }
  protected HashMap<String, OperatorSpec> featureLookup;
  protected HashMap<String, OperatorSpec> operatorLookup;
  protected List<TraversalSpec> traversals;
  protected Parameters parameters;

  public String getClassName(Node node) throws Exception {
    String operator = node.getOperator();

    if (operator.equals("feature")) {
      return getFeatureClassName(node.getNodeParameters());
    }

    OperatorSpec operatorType = operatorLookup.get(operator);

    if (operatorType == null) {
      return null;
    }

    // This is to compensate for the transparent behavior of the fitering nodes
    return operatorType.className;
  }

  public String getFeatureClassName(NodeParameters parameters) throws Exception {
    if (parameters.containsKey("class")) {
      return parameters.getString("class");
    }

    String name = parameters.get("name", parameters.get("default", (String) null));

    if (name == null) {
      throw new Exception(
              "Didn't find 'class', 'name', or 'default' parameter in this feature description.");
    }

    OperatorSpec operatorType = featureLookup.get(name);

    if (operatorType == null) {
      throw new Exception("Couldn't find a class for the feature named " + name + ".");
    }

    return operatorType.className;
  }

  @SuppressWarnings("unchecked")
  public Class<MovableIterator> getClass(Node node) throws Exception {
    String className = getClassName(node);
    if (className == null) {
      return null;
    }
    Class c = Class.forName(className);

    if (MovableIterator.class.isAssignableFrom(c)) {
      return (Class<MovableIterator>) c;
    } else {
      return null;
    }
  }

  public NodeType getNodeType(Node node) throws Exception {
    Class<MovableIterator> cls = getClass(node);
    if (cls != null) {
      return new NodeType(getClass(node));
    } else {
      return null;
    }
  }

  /**
   * Given a query node, generates the corresponding iterator object that can be
   * used for structured retrieval. This method just calls getClass() on the
   * node, then instantiates the resulting class.
   *
   * If the class returned by getClass() is a ScoringFunction, it must contain a
   * constructor that takes a single Parameters object. If the class returned by
   * getFeatureClass() is some kind of MovableIterator, it must take a
   * Parameters object and an ArrayList of DocumentDataIterators as parameters.
   */
  public MovableIterator getIterator(Node node, ArrayList<MovableIterator> childIterators) throws Exception {
    NodeType type = getNodeType(node);

    // One type of constructor allowed: Parameters?, NodeParameters?, child+
    // Anything not conforming to that gets an exception

    // Get the matching class for the node
    Class<? extends MovableIterator> c = getClass(node);
    if (c == null) {
      return null;
    }

    // There better be only 1 constructor
    Constructor[] cons = c.getConstructors();
    Constructor constructor;
    ArrayList<Object> arguments = new ArrayList<Object>();
    LinkedList<Class> formals = new LinkedList<Class>();
    boolean fail = false;
    int ic = 0;
    for (; ic < cons.length; ic++) {
      fail = false;
      constructor = cons[ic];
      arguments.clear();
      formals.clear();

      // Construct our argument list as we zip down the list of formal parameters
      formals.addAll(Arrays.asList(constructor.getParameterTypes()));
      int childIdx = 0;
      while (formals.size() > 0) {
        if (formals.get(0) == NodeParameters.class) {
          // Only valid if at the front or preceded by immutables
          if (arguments.isEmpty() || (arguments.size() == 1 && arguments.get(0) instanceof Parameters)) {
            NodeParameters params = node.getNodeParameters().clone();
            arguments.add(params);
          } else {
            fail = true;
            break;
          }
        } else if (MovableIterator.class.isAssignableFrom(formals.get(0))) {
          // Some number of MovableIterator, can be different - just do the one at the front now
          if (formals.get(0).isAssignableFrom(childIterators.get(childIdx).getClass())) {
            arguments.add(childIterators.get(childIdx));
            childIdx++;
          } else {
            fail = true;
            break;
          }
        } else if (formals.get(0).isArray()) {
          // Only an array of structured iterators - all the same type
          // First check that all children match
          Class ac = formals.get(0).getComponentType();
          for (int i = 0; i < childIterators.size(); i++) {
            if (!ac.isAssignableFrom(childIterators.get(i).getClass())) {
              fail = true;
              break;
            }
          }

          if (fail) {
            break;
          }
          Object typedArray = Array.newInstance(formals.get(0).getComponentType(), 0);
          Object[] generalArray = childIterators.toArray((Object[]) typedArray);
          arguments.add(generalArray);
        }
        formals.poll();
      }

      if (!fail) {
        break;
      }
    }

    if (fail) {
      StringBuilder msg = new StringBuilder();
      msg.append(String.format("No valid constructor for node %s.\n", node.toString()));
      msg.append("Allowable StructuredIterator constructors allow for leading optional Parameters,");
      msg.append(" followed by optional NodeParameters, and finally the list of child iterators.");
      throw new IllegalArgumentException(msg.toString());
    }

    return (MovableIterator) cons[ic].newInstance(arguments.toArray(new Object[0]));
  }

  public List<String> getTraversalNames() {
    ArrayList<String> result = new ArrayList<String>();
    for (TraversalSpec spec : traversals) {
      result.add(spec.className);
    }
    return result;
  }

  public List<Traversal> getTraversals(Retrieval retrieval)
          throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    ArrayList<Traversal> result = new ArrayList<Traversal>();
    for (TraversalSpec spec : traversals) {
      Class<? extends Traversal> traversalClass =
              (Class<? extends Traversal>) Class.forName(spec.className);
      Constructor<? extends Traversal> constructor = (Constructor<? extends Traversal>) traversalClass.getConstructors()[0];
      Traversal traversal;
      switch (constructor.getParameterTypes().length) {
        case 0:
          traversal = constructor.newInstance();
          break;
        case 1:
          traversal = constructor.newInstance(retrieval);
          break;
        default:
          throw new IllegalArgumentException("Traversals should not have more than 1 args : failed on " + traversalClass);
      }
      result.add(traversal);
    }

    return result;
  }
}
