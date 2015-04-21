// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.stats.*;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * The responsibility of the LocalRetrieval object is to provide a simpler
 * interface on top of the DiskIndex. Therefore, given a query or text string
 * representing a query, this object will perform the necessary transformations
 * to make it an executable object.
 *
 * 10/7/2010 - Modified for asynchronous execution
 *
 * @author trevor
 * @author irmarc
 * @author sjh
 */
public class LocalRetrieval implements Retrieval {

    protected final Logger logger = Logger.getLogger(this.getClass().getName());
    protected Index index;
    protected FeatureFactory features;
    protected Parameters globalParameters;
    protected CachedRetrieval cache;
    protected List<Traversal> defaultTraversals;

    /**
     * One retrieval interacts with one index. Parameters dictate the behavior
     * during retrieval time, and selection of the appropriate feature factory.
     * Additionally, the supplied parameters will be passed forward to the
     * chosen feature factory.
     */
    public LocalRetrieval(Index index) throws Exception {
        this(index, Parameters.create());
    }

    public LocalRetrieval(String filename) throws Exception {
        this(filename, Parameters.create());
    }

    public LocalRetrieval(String filename, Parameters parameters) throws Exception {
        this(new DiskIndex(filename), parameters);
    }

    public LocalRetrieval(Index index, Parameters parameters) throws Exception {
        this.globalParameters = parameters;
        setIndex(index);
    }

    protected void setIndex(Index indx) throws Exception {
        this.index = indx;
        features = new FeatureFactory(globalParameters);
        defaultTraversals = features.getTraversals(this);
        cache = null;
        if (this.globalParameters.get("cache", false)) {
            cache = new CachedRetrieval(this.globalParameters);
        }
    }

    /**
     * Closes the underlying index
     */
    @Override
    public void close() throws IOException {
        index.close();
    }

    /**
     * Returns some statistics about a particular index part -- vocab size,
     * number of entries, maximumDocCount of any indexed term, etc
     */
    @Override
    public IndexPartStatistics getIndexPartStatistics(String partName) throws IOException {
        return index.getIndexPartStatistics(partName);
    }

    @Override
    public Parameters getGlobalParameters() {
        return this.globalParameters;
    }

    /*
     * {
     * <partName> : { <nodeName> : <iteratorClass>, stemming : false, ... },
     * <partName> : { <nodeName> : <iteratorClass>, ... }, ... }
     */
    @Override
    public Parameters getAvailableParts() throws IOException {
        Parameters p = Parameters.create();
        for (String partName : index.getPartNames()) {
            Parameters inner = Parameters.create();
            Map<String, NodeType> nodeTypes = index.getPartNodeTypes(partName);
            for (String nodeName : nodeTypes.keySet()) {
                inner.set(nodeName, nodeTypes.get(nodeName).getIteratorClass().getName());
            }
            p.set(partName, inner);
        }
        return p;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public Document getDocument(String identifier, DocumentComponents p) throws IOException {
        return this.index.getDocument(identifier, p);
    }

    @Override
    public Map<String, Document> getDocuments(List<String> identifier, DocumentComponents p) throws IOException {
        return this.index.getDocuments(identifier, p);
    }

    /*
     * getArrayResults annotates a queue of scored documents returns an array
     *
     */
    protected <T extends ScoredDocument> T[] getArrayResults(T[] results, String indexId) throws IOException {
        assert (results != null); // unfortunately, we can't make an array of type T in java

        if (results.length == 0) {
            return results;
        }

        for (int i = 0; i < results.length; i++) {
            results[i].source = indexId;
            results[i].rank = i + 1;
        }

        // this is to assign proper document names
        T[] byID = Arrays.copyOf(results, results.length);

        Arrays.sort(byID, new Comparator<T>() {

            @Override
            public int compare(T o1, T o2) {
                return CmpUtil.compare(o1.document, o2.document);
            }
        });

        DataIterator<String> namesIterator = index.getNamesIterator();
        ScoringContext sc = new ScoringContext();

        for (T doc : byID) {
            namesIterator.syncTo(doc.document);
            sc.document = doc.document;

            if (doc.document == namesIterator.currentCandidate()) {
                doc.documentName = namesIterator.data(sc);

            } else {
                System.err.println("NAMES ITERATOR FAILED TO FIND DOCUMENT " + doc.document);
                // now throw an error.
                doc.documentName = index.getName(doc.document);
            }
        }

        return results;
    }

    @Override
    public Results executeQuery(Node queryTree) throws Exception {
        return executeQuery(queryTree, Parameters.create());
    }

    /**
     * Simple method to avoid boilerplate.
     */
    public Results transformAndExecuteQuery(Node queryTree) {
        try {
            Parameters qp = Parameters.create();
            return executeQuery(transformQuery(queryTree, qp), qp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Based on the root of the tree, that dictates how we execute.
    @Override
    public Results executeQuery(Node queryTree, Parameters queryParams) throws Exception {
        ScoredDocument[] results;
        if (globalParameters.containsKey("processingModel")) {
            queryParams.set("processingModel", globalParameters.getString("processingModel"));
        }
        ProcessingModel pm = ProcessingModel.create(this, queryTree, queryParams);

        // get some results
        results = pm.execute(queryTree, queryParams);
        if (results == null) {
            results = new ScoredDocument[0];
        }

        // Format and get names
        String indexId = this.globalParameters.get("indexId", "0");
        List<ScoredDocument> rankedList = Arrays.asList(getArrayResults(results, indexId));

        Results r = new Results();
        r.inputQuery = queryTree;
        r.processingModel = pm.getClass();
        r.scoredDocuments = rankedList;
        return r;
    }

    public BaseIterator createIterator(Parameters queryParameters, Node node) throws Exception {
        if (queryParameters.get("shareNodes", globalParameters.get("shareNodes", true))) {
            return createNodeMergedIterator(node, new HashMap<String, BaseIterator>());
        }
        return createNodeMergedIterator(node, null);
    }

    public BaseIterator createNodeMergedIterator(Node node,
            Map<String, BaseIterator> queryIteratorCache)
            throws Exception {
        ArrayList<BaseIterator> internalIterators = new ArrayList<>();
        BaseIterator iterator;

        // first check if this is a repeated node in this tree:
        if (queryIteratorCache != null && queryIteratorCache.containsKey(node.toString())) {
            iterator = queryIteratorCache.get(node.toString());
            return iterator;
        }

        // second check if this node is cached
        if (cache != null && cache.isCached(node)) {
            iterator = cache.getCachedIterator(node);
        } else {

      // otherwise we need to create a new iterator
            // start by recursively creating children
            for (Node internalNode : node.getInternalNodes()) {
                BaseIterator internalIterator = createNodeMergedIterator(internalNode, queryIteratorCache);
                internalIterators.add(internalIterator);
            }

            iterator = index.getIterator(node);
            if (iterator == null) {
                iterator = features.getIterator(node, internalIterators);
            }
        }

        // we've created a new iterator - add to the cache for future nodes
        if (queryIteratorCache != null) {
            queryIteratorCache.put(node.toString(), iterator);
        }

        return iterator;
    }

    @Override
    public Node transformQuery(Node queryTree, Parameters queryParams) throws Exception {
        return transformQuery(defaultTraversals, queryTree, queryParams);
    }

    private Node transformQuery(List<Traversal> traversals, Node queryTree, Parameters queryParams) throws Exception {
        for (Traversal traversal : traversals) {
      //System.out.println("Before:"+traversal.getClass());
            //System.out.println("Before:"+queryTree);
            queryTree = traversal.traverse(queryTree, queryParams);
      //System.out.println("After:"+traversal.getClass());
            //System.out.println("After:"+queryTree);
        }
        return queryTree;
    }

    @Override
    public FieldStatistics getCollectionStatistics(String nodeString) throws Exception {
        // first parse the node
        Node root = StructuredQuery.parse(nodeString);
        return getCollectionStatistics(root);
    }

    @Override
    public FieldStatistics getCollectionStatistics(Node root) throws Exception {

        String rootString = root.toString();
        if (cache != null && cache.cacheStats) {
            AggregateStatistic stat = cache.getCachedStatistic(rootString);
            if (stat != null && stat instanceof FieldStatistics) {
                return (FieldStatistics) stat;
            }
        }

        FieldStatistics s;
        // if you want passage statistics, you'll need a manual solution for now.
        ScoringContext sc = new ScoringContext();

        BaseIterator structIterator = createIterator(Parameters.create(), root);

        // first check if this iterator is an aggregate iterator (has direct access to stats)
        if (CollectionAggregateIterator.class.isInstance(structIterator)) {
            s = ((CollectionAggregateIterator) structIterator).getStatistics();

        } else if (structIterator instanceof LengthsIterator) {
            LengthsIterator iterator = (LengthsIterator) structIterator;
            s = new FieldStatistics();
            s.fieldName = root.toString();
            s.minLength = Integer.MAX_VALUE;

            while (!iterator.isDone()) {
                sc.document = iterator.currentCandidate();
                if (iterator.hasMatch(sc)) {
                    int len = iterator.length(sc);
                    s.collectionLength += len;
                    s.documentCount += 1;
                    s.nonZeroLenDocCount += (len > 0) ? 1 : 0;
                    s.maxLength = Math.max(s.maxLength, len);
                    s.minLength = Math.min(s.minLength, len);
                }
                iterator.movePast(sc.document);
            }

            s.avgLength = (s.documentCount > 0) ? (double) s.collectionLength / (double) s.documentCount : 0;
            s.minLength = (s.documentCount > 0) ? s.minLength : 0;
            return s;
        } else {
            throw new IllegalArgumentException("Node " + root.toString() + " is not a lengths iterator.");
        }

        if (cache != null && cache.cacheStats) {
            cache.addToCache(rootString, s);
        }

        return s;
    }

    @Override
    public NodeStatistics getNodeStatistics(String nodeString) throws Exception {
        // first parse the node
        Node root = StructuredQuery.parse(nodeString);
        return getNodeStatistics(root);
    }

    @Override
    public NodeStatistics getNodeStatistics(Node root) throws Exception {

        String rootString = root.toString();
        if (cache != null && cache.cacheStats) {
            AggregateStatistic stat = cache.getCachedStatistic(rootString);
            if (stat != null && stat instanceof NodeStatistics) {
                return (NodeStatistics) stat;
            }
        }

        NodeStatistics s;
        // if you want passage statistics, you'll need a manual solution for now.
        ScoringContext sc = new ScoringContext();

        BaseIterator structIterator = createIterator(Parameters.create(), root);

        if (NodeAggregateIterator.class.isInstance(structIterator)) {
            s = ((NodeAggregateIterator) structIterator).getStatistics();

        } else if (structIterator instanceof CountIterator) {

            s = new NodeStatistics();
            // set up initial values
            s.node = root.toString();
            s.nodeDocumentCount = 0;
            s.nodeFrequency = 0;
            s.maximumCount = 0;

            CountIterator iterator = (CountIterator) structIterator;

            while (!iterator.isDone()) {
                sc.document = iterator.currentCandidate();
                if (iterator.hasMatch(sc)) {
                    int c = iterator.count(sc);
                    s.nodeFrequency += iterator.count(sc);
                    s.maximumCount = Math.max(iterator.count(sc), s.maximumCount);
                    s.nodeDocumentCount += (c > 0) ? 1 : 0; // positive counting
                }
                iterator.movePast(iterator.currentCandidate());
            }

            return s;
        } else {
            // otherwise :
            throw new IllegalArgumentException("Node " + root.toString() + " is not a count iterator.");
        }

        if (cache != null && cache.cacheStats) {
            cache.addToCache(rootString, s);
        }

        return s;
    }

    @Override
    public NodeType getNodeType(Node node) throws Exception {
        NodeType nodeType = index.getNodeType(node);
        if (nodeType == null) {
            nodeType = features.getNodeType(node);
        }
        return nodeType;
    }

    @Override
    public QueryType getQueryType(Node node) throws Exception {
        if (node.getOperator().equals("text")) {
            return QueryType.UNKNOWN;
        }
        NodeType nodeType = getNodeType(node);
        Class outputClass = nodeType.getIteratorClass();
        if (ScoreIterator.class.isAssignableFrom(outputClass)
                || ScoringFunctionIterator.class.isAssignableFrom(outputClass)) {
            return QueryType.RANKED;
        } else if (IndicatorIterator.class.isAssignableFrom(outputClass)) {
            return QueryType.BOOLEAN;
        } else if (CountIterator.class.isAssignableFrom(outputClass)) {
            return QueryType.COUNT;
//    } else if (LengthsIterator.class.isAssignableFrom(outputClass)) {
//      return QueryType.LENGTH;
        } else {
            return QueryType.RANKED;
        }
    }

    @Override
    public Integer getDocumentLength(Integer docid) throws IOException {
        return index.getLength(docid);
    }

    @Override
    public Integer getDocumentLength(String docname) throws IOException {
        return index.getLength(index.getIdentifier(docname));
    }

    @Override
    public String getDocumentName(Integer docid) throws IOException {
        return index.getName(docid);
    }

    @Override
    public Long getDocumentId(String docname) throws IOException {
        return index.getIdentifier(docname);
    }

    public LengthsIterator getDocumentLengthsIterator() throws IOException {
        return index.getLengthsIterator();
    }

    public List<Long> getDocumentIds(List<String> docnames) throws IOException {
        List<Long> internalDocBuffer = new ArrayList<>();

        for (String name : docnames) {
            try {
                internalDocBuffer.add(index.getIdentifier(name));
            } catch (Exception e) {
                // arrays NEED to be aligned for good error detection
                internalDocBuffer.add(-1L);
            }
        }
        return internalDocBuffer;
    }

    @Override
    public void addNodeToCache(Node node) throws Exception {
        if (cache != null) {
            cache.addToCache(node, this.createIterator(Parameters.create(), node));
        }
    }

    @Override
    public void addAllNodesToCache(Node node) throws Exception {
        if (cache != null) {
            // recursivly add all nodes
            for (Node child : node.getInternalNodes()) {
                addAllNodesToCache(child);
            }

            cache.addToCache(node, this.createIterator(Parameters.create(), node));
        }
    }

    @Override
    public String toString() {
        return "LocalRetrieval(" + index.getIndexPath() + ")";
    }
}
