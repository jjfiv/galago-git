// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.iterator.bool.BinaryCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.bool.BooleanScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.bool.ExistentialIndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.bool.UniversalIndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.counts.CountsSumIterator;
import org.lemurproject.galago.core.retrieval.iterator.counts.LogCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.scoring.*;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.traversal.*;
import org.lemurproject.galago.core.retrieval.traversal.optimize.ExtentsToCountLeafTraversal;
import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenCombineTraversal;
import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenWindowTraversal;
import org.lemurproject.galago.core.retrieval.traversal.optimize.MergeCombineChildrenTraversal;
import org.lemurproject.galago.utility.Parameters;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author trevor
 * @author irmarc
 * @author sjh
 */
public class FeatureFactory {

  protected HashMap<String, OperatorSpec> operatorLookup;
  protected List<TraversalSpec> traversals;
  protected Parameters parameters;
  static String[][] sOperatorLookup = {
    // count iterators
    {MinCountIterator.class.getName(), "mincount"},
    {CountsSumIterator.class.getName(), "count-sum"},

    // extent iterators
    {SynonymIterator.class.getName(), "syn"},
    {SynonymIterator.class.getName(), "synonym"},
    {OrderedWindowIterator.class.getName(), "ordered"},
    {OrderedWindowIterator.class.getName(), "od"},
    {BigramIterator.class.getName(), "bigram"},
    {OrderedWindowIterator.class.getName(), "quote"}, // don't rely on this - ImplicitFeatureCast does: quote -> od:1 for now. (irmarc)
    {UnorderedWindowIterator.class.getName(), "unordered"},
    {UnorderedWindowIterator.class.getName(), "uw"},
    {UnorderedWindowBigramIterator.class.getName(), "ubigram"},
    {UnorderedWindowIterator.class.getName(), "intersect"},
    {NullExtentIterator.class.getName(), "null"},

    // score iterators
    {ScoreCombinationIterator.class.getName(), "combine"},
    {WeightedSumIterator.class.getName(), "wsum"},
    {ThresholdIterator.class.getName(), "threshold"}, // score -> indicator
    {ScaleIterator.class.getName(), "scale"},
    {ScoreCombinationIterator.class.getName(), "rm"},
    {ScoreCombinationIterator.class.getName(), "bm25rf"},
    {BM25FCombinationIterator.class.getName(), "bm25fcomb"},
    {BM25FCombinationIterator.class.getName(), "bm25fcomb"},
    {LogProbNotIterator.class.getName(), "logprobnot"},

    // count -> score iterators
    // Scorers can be named directly as nodes
    {DirichletScoringIterator.class.getName(), "dirichlet"},
    {JelinekMercerScoringIterator.class.getName(), "linear"},
    {JelinekMercerScoringIterator.class.getName(), "jm"},
    {BM25ScoringIterator.class.getName(), "bm25"},
    {BM25RFScoringIterator.class.getName(), "bm25rf"},
    {BoostingIterator.class.getName(), "boost"},
    {LogarithmIterator.class.getName(), "log"},
    {PL2ScoringIterator.class.getName(), "pl2"},
    {InL2ScoringIterator.class.getName(), "inl2"},
    {BiL2ScoringIterator.class.getName(), "bil2"},
    {LogCountIterator.class.getName(), "log-count"},

    // indicator iterators
    {UniversalIndicatorIterator.class.getName(), "all"},
    {UniversalIndicatorIterator.class.getName(), "band"},
    {ExistentialIndicatorIterator.class.getName(), "any"},
    {ExistentialIndicatorIterator.class.getName(), "bor"},

    // indicator -> count iterators
    {BinaryCountIterator.class.getName(), "bool-to-count"},
    // indicator -> score iterators
    {BooleanScoreIterator.class.getName(), "bool"},

    // generic iterators
    {RequireIterator.class.getName(), "require"},
    {RejectIterator.class.getName(), "reject"},

    //field iterators
    {ExtentInsideIterator.class.getName(), "inside"},
    {GreaterThanIterator.class.getName(), "greater"},
    {LessThanIterator.class.getName(), "less"},
    {InBetweenIterator.class.getName(), "between"},
    {EqualityIterator.class.getName(), "equals"},

    //pasage iterators
    {PassageFilterIterator.class.getName(), "passagefilter"},
    {PL2ScoringIterator.class.getName(), "pl2scorer"},
    {PassageLengthIterator.class.getName(), "passagelengths"},

  };
  static String[] sTraversalList = {
    RelevanceModelTraversal.class.getName(),
    ReplaceOperatorTraversal.class.getName(),
    StopStructureTraversal.class.getName(),
    StopWordTraversal.class.getName(),
    WeightedSequentialDependenceTraversal.class.getName(),
    WeightedSequentialDependenceTraversal.class.getName(),
    SequentialDependenceTraversal.class.getName(),
    FullDependenceTraversal.class.getName(),
    ProximityDFRTraversal.class.getName(),
    PRMS2Traversal.class.getName(),
    TransformRootTraversal.class.getName(),
    WindowRewriteTraversal.class.getName(),
    TextFieldRewriteTraversal.class.getName(),
    PartAssignerTraversal.class.getName(),
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
    this(p, sOperatorLookup, sTraversalList);
  }

  public FeatureFactory(Parameters parameters,
          String[][] sOperatorLookup,
          String[] sTraversalList) {
    operatorLookup = new HashMap<>();
    this.parameters = parameters;

    for (String[] item : sOperatorLookup) {
      OperatorSpec operator = new OperatorSpec();
      operator.className = item[0];
      String operatorName = item[1];
      operatorLookup.put(operatorName, operator);
    }

    ArrayList<TraversalSpec> afterTraversals = new ArrayList<>();
    ArrayList<TraversalSpec> beforeTraversals = new ArrayList<>();
    ArrayList<TraversalSpec> insteadTraversals = new ArrayList<>();

    if (parameters.isMap("traversals") || parameters.isList("traversals", Parameters.class)) {
      List<Parameters> traversals = parameters.getAsList("traversals", Parameters.class);
      for (Parameters traversal : traversals) {
        String className = traversal.getString("name");
        String order = traversal.get("order", "after");

        TraversalSpec spec = new TraversalSpec();
        spec.className = className;

        switch (order) {
          case "before":
            beforeTraversals.add(spec);
            break;
          case "after":
            afterTraversals.add(spec);
            break;
          case "instead":
            insteadTraversals.add(spec);
            break;
          default:
            throw new RuntimeException("order must be one of {before,after,instead}");
        }
      }
    }

    // If the user doesn't want to replace the current pipeline, add in the std pipeline
    if (insteadTraversals.size() == 0) {
      for (String className : sTraversalList) {
        TraversalSpec spec = new TraversalSpec();
        spec.className = className;
        insteadTraversals.add(spec);
      }
    }


    traversals = new ArrayList<>();
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
  }

  public static class OperatorSpec {

    public String className;
  }

  public static class TraversalSpec {

    public String className;
  }

  public String getClassName(Node node) throws Exception {
    String operator = node.getOperator();

    OperatorSpec operatorType = operatorLookup.get(operator);

    if (operatorType == null) {
      return null;
    }

    // This is to compensate for the transparent behavior of the fitering nodes
    return operatorType.className;
  }

  @SuppressWarnings("unchecked")
  public Class<BaseIterator> getClass(Node node) throws Exception {
    String className = getClassName(node);
    if (className == null) {
      return null;
    }
    Class c = Class.forName(className);

    if (BaseIterator.class.isAssignableFrom(c)) {
      return (Class<BaseIterator>) c;
    } else {
      return null;
    }
  }

  public NodeType getNodeType(Node node) throws Exception {
    Class<BaseIterator> cls = getClass(node);
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
   * getFeatureClass() is some kind of Iterator, it must take a
   * Parameters object and an ArrayList of DocumentDataIterators as parameters.
   */
  public BaseIterator getIterator(Node node, ArrayList<BaseIterator> childIterators) throws Exception {
    // One type of constructor allowed: Parameters?, NodeParameters?, child+
    // Anything not conforming to that gets an exception

    // Get the matching class for the node
    Class<? extends BaseIterator> c = getClass(node);
    if (c == null) {
      throw new IllegalArgumentException("Couldn't get the class of node: "+node);
    }

    // There better be only 1 constructor
    Constructor[] cons = c.getConstructors();
    Constructor constructor;
    ArrayList<Object> arguments = new ArrayList<>();
    LinkedList<Class<?>> formals = new LinkedList<>();
    boolean fail = false;
    String failStr = "<unknown>";
    int ic = 0;
    for (; ic < cons.length; ic++) {
      fail = false;
      constructor = cons[ic];
      arguments.clear();
      formals.clear();

      // Construct our argument list as we zip down the list of formal parameters
      formals.addAll(Arrays.<Class<?>>asList(constructor.getParameterTypes()));
      int childIdx = 0;
      while (formals.size() > 0) {
        if (formals.get(0) == NodeParameters.class) {
          // Only valid if at the front or preceded by immutables
          if (arguments.isEmpty()) {
            NodeParameters params = node.getNodeParameters().clone();
            arguments.add(params);
          } else {
            fail = true;
            failStr = "NodeParameters must be the first argument, not the " + arguments.size() + "-th.";
            break;
          }
        } else if (BaseIterator.class.isAssignableFrom(formals.get(0)) && childIterators.size() > childIdx) {
          // Some number of Iterator, can be different - just do the one at the front now
          // only if we have a childIterator left to assign!
          if (formals.get(0).isAssignableFrom(childIterators.get(childIdx).getClass())) {
            arguments.add(childIterators.get(childIdx));
            childIdx++;
          } else {
            fail = true;
            failStr = "Argument " + arguments.size() + " is:\n" + childIterators.get(childIdx).getClass().getName() + "\nConstructor expected:\n" + formals.get(0).getName();
            break;
          }
        } else if (formals.get(0).isArray()) {
          // Only an array of structured iterators - all the same type
          // First check that all children match
          Class<?> ac = formals.get(0).getComponentType();
          for (int i = childIdx; i < childIterators.size(); i++) {
            if (!ac.isAssignableFrom(childIterators.get(i).getClass())) {
              fail = true;
              failStr = "Argument " + arguments.size() + " is:\n" + childIterators.get(i).getClass().getName() + "\nConstructor expected an array of:\n" + ac.getName();
              break;
            }
          }

          if (fail) {
            break;
          }
          Object typedArray = Array.newInstance(formals.get(0).getComponentType(), 0);
          Object[] generalArray = childIterators.subList(childIdx, childIterators.size()).toArray((Object[]) typedArray);
          arguments.add(generalArray);
        }
        formals.poll();
      }

      if (!fail) {
        break;
      }
    }

    if (fail) {
      throw new IllegalArgumentException(
        String.format("No valid constructor for node %s.\n", node.toString()) +
          "Allowable Iterator constructors allow for leading optional Parameters," +
          " followed by optional NodeParameters, and finally the list of child iterators." +
          "FAILED AT: " + failStr);
    }

    try {
      return (BaseIterator) cons[ic].newInstance(arguments.toArray(new Object[arguments.size()]));
    } catch (Exception ex) {
      throw new RuntimeException("Couldn't instantiate class: "+c.getName(), ex);
    }
  }

  public List<String> getTraversalNames() {
    ArrayList<String> result = new ArrayList<>();
    for (TraversalSpec spec : traversals) {
      result.add(spec.className);
    }
    return result;
  }

  public List<Traversal> getTraversals(Retrieval retrieval)
          throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    ArrayList<Traversal> result = new ArrayList<>();
    
    for (TraversalSpec spec : traversals) {
      Class<? extends Traversal> traversalClass =
              (Class<? extends Traversal>) Class.forName(spec.className);
      Constructor[] constructors = traversalClass.getConstructors();
      Traversal traversal = null;
      
      // try to construct a traversal with a retrieval
      for (Constructor c : constructors) {
        Class<?>[] argTypes = c.getParameterTypes();
        if (argTypes.length == 1 && argTypes[0].isAssignableFrom(Retrieval.class)) {
          traversal = (Traversal) c.newInstance(retrieval);
          break;
        }
      }
      
      // allow a traversal with no retrieval
      if(traversal == null) {
        for(Constructor c : constructors) {
          Class[] argTypes = c.getParameterTypes();
          if (argTypes.length == 0) {
            traversal = (Traversal) c.newInstance();
            break;
          }
        }
      }
      
      if(traversal == null) {
        throw new IllegalArgumentException("Traversals should have obvious constructors, failed on: " + traversalClass);
      }

      result.add(traversal);
    }

    return result;
  }
}
