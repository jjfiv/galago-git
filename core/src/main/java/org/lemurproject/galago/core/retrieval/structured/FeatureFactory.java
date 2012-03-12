// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import java.io.IOException;
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
import org.lemurproject.galago.core.retrieval.iterator.BM25FCombinationIterator;
import org.lemurproject.galago.core.retrieval.iterator.BM25FieldScoringIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.BM25RFScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.BM25ScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.BoostingIterator;
import org.lemurproject.galago.core.retrieval.iterator.DFRScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.DirichletProbabilityScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.DirichletScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.EqualityIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExistentialIndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentInsideIterator;
import org.lemurproject.galago.core.retrieval.iterator.GreaterThanIterator;
import org.lemurproject.galago.core.retrieval.iterator.InBetweenIterator;
import org.lemurproject.galago.core.retrieval.iterator.InverseDocFrequencyIterator;
import org.lemurproject.galago.core.retrieval.iterator.JelinekMercerProbabilityScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.JelinekMercerScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.LessThanIterator;
import org.lemurproject.galago.core.retrieval.iterator.LogarithmIterator;
import org.lemurproject.galago.core.retrieval.iterator.OrderedWindowIterator;
import org.lemurproject.galago.core.retrieval.iterator.RequireIterator;
import org.lemurproject.galago.core.retrieval.iterator.RejectIterator;
import org.lemurproject.galago.core.retrieval.iterator.NullExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.PL2FieldScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.PassageFilterIterator;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.core.retrieval.iterator.ScaleIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreCombinationIterator;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.core.retrieval.iterator.SynonymIterator;
import org.lemurproject.galago.core.retrieval.iterator.ThresholdIterator;
import org.lemurproject.galago.core.retrieval.iterator.UniversalIndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.UnorderedWindowIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.traversal.BM25RelevanceFeedbackTraversal;
import org.lemurproject.galago.core.retrieval.traversal.AnnotateCollectionStatistics;
import org.lemurproject.galago.core.retrieval.traversal.BM25FTraversal;
import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenWindowTraversal;
import org.lemurproject.galago.core.retrieval.traversal.FullDependenceTraversal;
import org.lemurproject.galago.core.retrieval.traversal.ImplicitFeatureCastTraversal;
import org.lemurproject.galago.core.retrieval.traversal.IndriWeightConversionTraversal;
import org.lemurproject.galago.core.retrieval.traversal.IndriWindowCompatibilityTraversal;
import org.lemurproject.galago.core.retrieval.traversal.InsideToFieldPartTraversal;
import org.lemurproject.galago.core.retrieval.traversal.PL2FTraversal;
import org.lemurproject.galago.core.retrieval.traversal.PRMS2Traversal;
import org.lemurproject.galago.core.retrieval.traversal.PRMSTraversal;
import org.lemurproject.galago.core.retrieval.traversal.RelevanceModelTraversal;
import org.lemurproject.galago.core.retrieval.traversal.RemoveStopwordsTraversal;
import org.lemurproject.galago.core.retrieval.traversal.SequentialDependenceTraversal;
import org.lemurproject.galago.core.retrieval.traversal.TextFieldRewriteTraversal;
import org.lemurproject.galago.core.retrieval.traversal.TransformRootTraversal;
import org.lemurproject.galago.core.retrieval.traversal.WeightedDependenceTraversal;
import org.lemurproject.galago.core.retrieval.traversal.WindowRewriteTraversal;
import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenCombineTraversal;
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
    {SynonymIterator.class.getName(), "syn"},
    {SynonymIterator.class.getName(), "synonym"},
    {ExtentInsideIterator.class.getName(), "inside"},
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
    {PassageFilterIterator.class.getName(), "passagefilter"}
  };
  static String[][] sFeatureLookup = {
    {DirichletProbabilityScoringIterator.class.getName(), "dirichlet-raw"},
    {JelinekMercerProbabilityScoringIterator.class.getName(), "linear-raw"},
    {JelinekMercerProbabilityScoringIterator.class.getName(), "jm-raw"},
    {DirichletScoringIterator.class.getName(), "dirichlet"},
    {JelinekMercerScoringIterator.class.getName(), "linear"},
    {JelinekMercerScoringIterator.class.getName(), "jm"},
    {BM25ScoringIterator.class.getName(), "bm25"},
    {BM25RFScoringIterator.class.getName(), "bm25rf"},
    {BoostingIterator.class.getName(), "boost"},
    {BM25FieldScoringIterator.class.getName(), "bm25f"},
    {InverseDocFrequencyIterator.class.getName(), "idf"},
    {LogarithmIterator.class.getName(), "log"},
    {DFRScoringIterator.class.getName(), "dfr"},
    {PL2FieldScoringIterator.class.getName(), "pl2f"}
  };

  public FeatureFactory(Parameters p) {
    this(p, sOperatorLookup, sFeatureLookup);
  }

  public FeatureFactory(Parameters parameters,
          String[][] sOperatorLookup, String[][] sFeatureLookup) {
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

    afterTraversals = new ArrayList<TraversalSpec>();
    beforeTraversals = new ArrayList<TraversalSpec>();
    insteadTraversals = new ArrayList<TraversalSpec>();

    if (parameters.containsKey("traversals")) {
      Parameters traversals = parameters.getMap("traversals");
      for (String className : traversals.getKeys()) {
        String order = traversals.get(className, "after");
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
  protected List<TraversalSpec> beforeTraversals, insteadTraversals, afterTraversals;
  protected Parameters parameters;

  public String getClassName(Node node) throws Exception {
    String operator = node.getOperator();

    if (operator.equals("feature")) {
      return getFeatureClassName(node.getNodeParameters());
    }

    OperatorSpec operatorType = operatorLookup.get(operator);

    if (operatorType == null) {
      throw new BadOperatorException("Unknown operator name: #" + operator);
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
  public Class<StructuredIterator> getClass(Node node) throws Exception {
    String className = getClassName(node);
    Class c = Class.forName(className);

    if (StructuredIterator.class.isAssignableFrom(c)) {
      return (Class<StructuredIterator>) c;
    } else {
      throw new Exception("Found a class, but it's not a StructuredIterator: " + className);
    }
  }

  public NodeType getNodeType(Node node) throws Exception {
    return new NodeType(getClass(node));
  }

  /**
   * Given a query node, generates the corresponding iterator object that can be used
   * for structured retrieval.  This method just calls getClass() on the node,
   * then instantiates the resulting class.
   *
   * If the class returned by getClass() is a ScoringFunction, it must contain
   * a constructor that takes a single Parameters object.  If the class returned by
   * getFeatureClass() is some kind of StructuredIterator,
   * it must take a Parameters object and an ArrayList of DocumentDataIterators as globals.
   */
  public StructuredIterator getIterator(Node node, ArrayList<StructuredIterator> childIterators) throws Exception {
    NodeType type = getNodeType(node);

    // One type of constructor allowed: Parameters?, NodeParameters?, child+
    // Anything not conforming to that gets an exception

    // Get the matching class for the node
    Class<? extends StructuredIterator> c = getClass(node);
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

      // Construct our argument list as we zip down the list of formal globals
      formals.addAll(Arrays.asList(constructor.getParameterTypes()));
      int childIdx = 0;
      while (formals.size() > 0) {
        if (formals.get(0) == Parameters.class) {
          // dealing w/ immutable globals  -- front only
          if (arguments.isEmpty()) {
            arguments.add(this.parameters);
          } else {
            fail = true;
            break;
          }
        } else if (formals.get(0) == NodeParameters.class) {
          // Only valid if at the front or preceded by immutables
          if (arguments.isEmpty() || (arguments.size() == 1 && arguments.get(0) instanceof Parameters)) {
            NodeParameters params = node.getNodeParameters().clone();
            arguments.add(params);
          } else {
            fail = true;
            break;
          }
        } else if (StructuredIterator.class.isAssignableFrom(formals.get(0))) {
          // Some number of structurediterators, can be different - just do the one at the front now
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

    return (StructuredIterator) cons[ic].newInstance(arguments.toArray(new Object[0]));
  }

  public List<Traversal> getTraversals(Retrieval retrieval, Node queryTree, Parameters queryParams)
          throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
    ArrayList<Traversal> result = new ArrayList<Traversal>();

    // Do "before" traversals
    if (beforeTraversals.size() > 0) {
      result.addAll(getTraversalsFromSpecs(beforeTraversals, retrieval, queryTree, queryParams));
    }

    // Do either the builtins of the "instead" traversals
    if (insteadTraversals.size() == 0) {
      result.addAll(constructBuiltInTraversals(retrieval, queryTree, queryParams));
    } else {
      result.addAll(getTraversalsFromSpecs(insteadTraversals, retrieval, queryTree, queryParams));
    }

    // Do "after" traversals
    if (afterTraversals.size() > 0) {
      result.addAll(getTraversalsFromSpecs(afterTraversals, retrieval, queryTree, queryParams));
    }

    return result;
  }

  // Hardcode the builtins for speed - if we're executing a bunch of queries, let's not have the
  // overhead take up too much time.
  private List<Traversal> constructBuiltInTraversals(Retrieval r, Node root, Parameters qp)
          throws IOException {
    ArrayList<Traversal> traversals = new ArrayList<Traversal>();
    if (WeightedDependenceTraversal.isNeeded(root)) {
      traversals.add(new WeightedDependenceTraversal(r));
    }
    if (SequentialDependenceTraversal.isNeeded(root)) {
      traversals.add(new SequentialDependenceTraversal(r));
    }    
    if (FullDependenceTraversal.isNeeded(root)) {
      traversals.add(new FullDependenceTraversal(r));
    }
    if (TransformRootTraversal.isNeeded(root)) {
      traversals.add(new TransformRootTraversal(r));
    }
    if (PRMSTraversal.isNeeded(root)) {
      traversals.add(new PRMSTraversal(r));
    }
    if (PRMS2Traversal.isNeeded(root)) {
      traversals.add(new PRMS2Traversal(r));
    }
    if (BM25FTraversal.isNeeded(root)) {
      traversals.add(new BM25FTraversal(r));
    }
    if (PL2FTraversal.isNeeded(root)) {
      traversals.add(new PL2FTraversal(r));
    }
    if (WindowRewriteTraversal.isNeeded(root)) {
      traversals.add(new WindowRewriteTraversal(r));
    }
    if (IndriWeightConversionTraversal.isNeeded(root)) {
      traversals.add(new IndriWeightConversionTraversal());
    }
    if (IndriWindowCompatibilityTraversal.isNeeded(root)) {
      traversals.add(new IndriWindowCompatibilityTraversal());
    }
    if (TextFieldRewriteTraversal.isNeeded(root)) {
      traversals.add(new TextFieldRewriteTraversal(r));
    }
    if (InsideToFieldPartTraversal.isNeeded(root)) {
      traversals.add(new InsideToFieldPartTraversal(r));
    }
    if (ImplicitFeatureCastTraversal.isNeeded(root)) {
      traversals.add(new ImplicitFeatureCastTraversal(r));
    }
    if (RemoveStopwordsTraversal.isNeeded(root)) {
      traversals.add(new RemoveStopwordsTraversal(r));
    }
    if (FlattenWindowTraversal.isNeeded(root)) {
      traversals.add(new FlattenWindowTraversal());
    }
    if (FlattenCombineTraversal.isNeeded(root)) {
      traversals.add(new FlattenCombineTraversal());
    }
    if (MergeCombineChildrenTraversal.isNeeded(root)) {
      traversals.add(new MergeCombineChildrenTraversal());
    }
    if (RelevanceModelTraversal.isNeeded(root)) {
      traversals.add(new RelevanceModelTraversal(r));
    }
    if (BM25RelevanceFeedbackTraversal.isNeeded(root)) {
      traversals.add(new BM25RelevanceFeedbackTraversal(r));
    }
    if (AnnotateCollectionStatistics.isNeeded(root)) {
      traversals.add(new AnnotateCollectionStatistics(r));
    }
    return traversals;
  }

  private List<Traversal> getTraversalsFromSpecs(List<TraversalSpec> specs, Retrieval retrieval, Node queryTree, Parameters queryParams)
          throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    ArrayList<Traversal> result = new ArrayList<Traversal>();
    for (TraversalSpec spec : specs) {
      Class<? extends Traversal> traversalClass =
              (Class<? extends Traversal>) Class.forName(spec.className);
      if (((Boolean) traversalClass.getMethod("isNeeded", Node.class).invoke(null, queryTree)).booleanValue()) {
        Constructor<? extends Traversal> constructor = (Constructor<? extends Traversal>) traversalClass.getConstructors()[0];
        Traversal traversal;
        switch (constructor.getParameterTypes().length) {
          case 0:
            traversal = constructor.newInstance();
            break;
          case 1:
            traversal = constructor.newInstance(retrieval);
            break;
          case 2:
            traversal = constructor.newInstance(retrieval, queryParams);
            break;
          default:
            throw new IllegalArgumentException("Traversals should not have more than 2 args.");
        }
        result.add(traversal);
      }
    }

    return result;
  }
}
