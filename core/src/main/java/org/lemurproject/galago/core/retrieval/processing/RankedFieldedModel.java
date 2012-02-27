// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.FieldLengthsReader;
import org.lemurproject.galago.core.index.disk.WindowIndexReader;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Performs straightforward document-at-a-time (daat) processing of a fully annotated query,
 * processing scores over documents. The modification here is the presence of
 * field-level lengths, meaning they need to be maintained and updated.
 *
 * @author irmarc
 */
public class RankedFieldedModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  int[] whitelist;
  HashMap<String, LengthsReader.Iterator> lReaders;

  public RankedFieldedModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
    whitelist = null;
  }

  @Override
  public void defineWorkingSet(int[] docs) {
    whitelist = docs;
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    if (whitelist == null) {
      return executeWholeCollection(queryTree, queryParams);
    } else {
      return executeWorkingSet(queryTree, queryParams);
    }
  }

  private ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
          throws Exception {
    // This model uses the simplest ScoringContext
    FieldScoringContext context = new FieldScoringContext();
    initializeFieldLengths(context);
    
    // have to be sure
    Arrays.sort(whitelist);

    // construct the query iterators
    MovableScoreIterator iterator = (MovableScoreIterator) retrieval.createIterator(queryTree, context);
    int requested = (int) queryParams.get("requested", 1000);

    // now there should be an iterator at the root of this tree
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();
    LengthsReader.Iterator lengthsIterator = index.getLengthsIterator();

    for (int i = 0; i < whitelist.length; i++) {
      int document = whitelist[i];
      iterator.moveTo(document);
      lengthsIterator.moveTo(document);
      int length = lengthsIterator.getCurrentLength();
      this.updateFieldLengths(context, document);
      // This context is shared among all scorers
      context.document = document;
      context.length = length;
      double score = iterator.score();
      if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
        ScoredDocument scoredDocument = new ScoredDocument(document, score);
        queue.add(scoredDocument);
        if (requested > 0 && queue.size() > requested) {
          queue.poll();
        }
      }
    }
    return toReversedArray(queue);
  }

  private ScoredDocument[] executeWholeCollection(Node queryTree, Parameters queryParams)
          throws Exception {

    // This model uses the simplest ScoringContext
    FieldScoringContext context = new FieldScoringContext();
    initializeFieldLengths(context);
    
    // Number of documents requested.
    int requested = (int) queryParams.get("requested", 1000);

    // Maintain a queue of candidates
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);

    // Need to maintain document lengths
    LengthsReader.Iterator lengthsIterator = index.getLengthsIterator();

    // construct the iterators -- we use tree processing
    MovableScoreIterator iterator = (MovableScoreIterator) retrieval.createIterator(queryTree, context);

    // now there should be an iterator at the root of this tree
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      lengthsIterator.moveTo(document);
      int length = lengthsIterator.getCurrentLength();
      updateFieldLengths(context, document);
      // This context is shared among all scorers
      context.document = document;
      context.length = length;
      if (iterator.atCandidate(document)) {
        double score = iterator.score();
        if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
          ScoredDocument scoredDocument = new ScoredDocument(document, score);
          queue.add(scoredDocument);
          if (requested > 0 && queue.size() > requested) {
            queue.poll();
          }
        }
      }
      iterator.next();
    }
    return toReversedArray(queue);
  }

  protected void updateFieldLengths(FieldScoringContext context, int currentDoc) throws IOException {
    // Now get updated counts                                                                                                               
    for (Map.Entry<String, LengthsReader.Iterator> entry : lReaders.entrySet()) {
      if (entry.getValue().moveTo(currentDoc)) {
        context.lengths.put(entry.getKey(), entry.getValue().getCurrentLength());
      } else {
        context.lengths.put(entry.getKey(), 0);
      }
    }
  }

  protected void initializeFieldLengths(FieldScoringContext context) throws IOException {

    lReaders = new HashMap<String, LengthsReader.Iterator>();
    Parameters global = retrieval.getGlobalParameters();
    List<String> fields;
    if (global.containsKey("fields")) {
      fields = global.getAsList("fields");
    } else {
      fields = new ArrayList<String>();
    }

    DiskIndex index = (DiskIndex) retrieval.getIndex();

    WindowIndexReader wir = (WindowIndexReader) index.openLocalIndexPart("extents");
    FieldLengthsReader flr = new FieldLengthsReader(wir);
    Parameters parts = retrieval.getAvailableParts();
    for (String field : fields) {
      String partName = "field." + field;
      if (!parts.containsKey(partName)) {
        continue;
      }
      context.lengths.put(field, 0);
      LengthsReader.Iterator it = flr.getLengthsIterator(field);
      lReaders.put(field, it);
    }
  }
}
