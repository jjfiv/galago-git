// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.parse;

import org.lemurproject.galago.core.types.DocumentTermInfo;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.contrib.parse.DocTermsInfo;
import org.lemurproject.galago.contrib.parse.DocTermsInfo.TermInfo;
import org.lemurproject.galago.contrib.parse.DocTermsInfo.PositionInfo;
import org.lemurproject.galago.contrib.index.disk.ForwardIndexSerializer;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.IOException;
import java.util.logging.Logger;


/**
 * Take sorted output from ForwardIndexGenerate and accumulate position 
 * information for each term.  Create a KeyValuePair object, with doc ID
 * key and DocumentTermsInfo object as value.
 *
 * The DocTermInfo input object is in the following form
 *       docid  term  beginPosition  endPosition  beginOffset  endOffset
 *
 * Resulting output will be a DocTermsInfo object of the form 
 *   Doc Stats    : docid  termcount (total terms in doc) and uniquecount
 *   Term Info    : term  termFreq  maxTF
 *   Position Info: [bPos  ePos  bOffs  eOffs]+
 *                  We will ignore end positions and offsets when writing 
 *                  to an index since ending information can be calculated
 *                  as ePos = bPos + 1 and eOffs = bOffs + term.length().
 *
 *
 * @author smh
 */

@Verified
@InputClass (className = "org.lemurproject.galago.core.types.DocumentTermInfo", order = {"+docid", "+term", "+begPos"})
@OutputClass (className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})


public class ForwardIndexReduce extends StandardStep<DocumentTermInfo, KeyValuePair> {

  private   String                indexRoot;
  protected TupleFlowParameters tfParameters;
  protected Parameters          parameters;
  private   Counter               fwindexReduceCounter;

  private long lastDocID;
  private String lastTerm;
  private DocTermsInfo docTermsInfo;
  private DocTermsInfo lastDocTermsInfo;
  private DocTermsInfo currentDocTermsInfo;
    
  //private DocTermsInfo.TermInfo docTermInfo;
  private TermInfo docTermInfo;
  private TermInfo lastTermInfo;

  //private DocTermsInfo.PositionInfo positionInfo;
  private PositionInfo positionInfo;

  //- Total number of docs in the collection  (keyCount)
  protected long docsInCollection;

  //- Total number of terms in the collection
  protected long totalTermsInCollection;

  //- Total terms in doc
  protected int totalTermsInDoc;

  //- Max frequency of a term in the collection
  protected long maxTermFreqInCollection;

  //- Max frequency of a term in the doc
  protected int maxTermFreqInDoc;

  //- Total number of unique terms in the collection
  //protected long uniqueTermsInCollection;

  //- Unique terms in doc
  protected int uniqueTermsInDoc;

  private static final Logger FWINDEXREDUCELOG = Logger.getLogger (ForwardIndexReduce.class.getSimpleName ());


  public ForwardIndexReduce (TupleFlowParameters parameters) {

    this.tfParameters = parameters;
    this.parameters = parameters.getJSON ();
    this.indexRoot = tfParameters.getJSON ().get ("indexPath", "/home/harding/work/idx/ap89_fields.idx");
    this.fwindexReduceCounter = parameters.getCounter ("FWIndex Terms Reduced");

    this.lastDocID = -1L;
    this.lastTerm = null;
    this.lastDocTermsInfo = null;
    this.lastTermInfo = null;
    this.uniqueTermsInDoc = 0;
    this.totalTermsInDoc = 0;
    this.maxTermFreqInDoc = 0;

    this.docsInCollection = 0L;
    //this.uniqueTermsInCollection = 0L;
    this.totalTermsInCollection = 0L;
    this.maxTermFreqInCollection = 0;
  }

    
  public void process (DocumentTermInfo dti) throws IOException {

    int termCount = 0;

    DocTermsInfo docTermsInfo = null;
    TermInfo termInfo = null;
    PositionInfo positionInfo = null;

    long   docid   = dti.docid;
    String term    = dti.term;
    int    begPos  = dti.begPos;
    //int    endPos  = dti.endPos;  
    int    begOffs = dti.begOffs;
    //int    endOffs = dti.endOffs;

    //FWINDEXREDUCELOG.info ("ForwardIndexReduce ==> Doc ID: " + docid);

    //- The first document being processed.
    if (lastDocID == -1L) {
      docTermsInfo = new DocTermsInfo (docid);
      docsInCollection++;

      //***
      //this.docsInCollectionCounter.increment ();
      //parameters.set ("docsInCollection", docsInCollection);
      this.tfParameters.getJSON ().set ("docsInCollection", docsInCollection);
      
      termInfo = new TermInfo (term);
      totalTermsInDoc++;
      totalTermsInCollection++;


      //***
      //this.totalTermsInCollectionCounter.increment ();
      parameters.set ("totalTermsInCollection", totalTermsInCollection);
      
      //uniqueTermsInCollection++;
      uniqueTermsInDoc++;
      
      //positionInfo = new PositionInfo (begPos, endPos, begOffs, endOffs);
      positionInfo = new PositionInfo (begPos, begOffs);
      termInfo.positionInfoList.add (positionInfo);
 
      lastDocID = docid;
      lastTerm = term;
      lastTermInfo = termInfo;
      lastDocTermsInfo = docTermsInfo;
    }
    else if (docid == lastDocID) {
      docTermsInfo = lastDocTermsInfo;
	
      //- If same term in same doc, just add to the term positions list
      if (term.equals (lastTerm)) {
        termInfo = lastTermInfo;
        //totalTermsInCollection++;
        totalTermsInDoc++;
	  
        //- Add more positions to existing term list
        //positionInfo = new PositionInfo (begPos, endPos, begOffs, endOffs);
        positionInfo = new PositionInfo (begPos, begOffs);
        termInfo.positionInfoList.add (positionInfo);
      }

      //- If same doc but new term, save existing term info to terms in doc list
      //  then create a new term info for new term
      else {
        termInfo = lastTermInfo;

        //- Update current term info and save to doc terms list
        termCount = termInfo.positionInfoList.size();
        termInfo.termFreq = termCount;

	maxTermFreqInDoc = Math.max (maxTermFreqInDoc, termCount);
        //maxTermFreqInCollection = Math.max (maxTermFreqInCollection, termCount);

        docTermsInfo.termsInfoHM.put (lastTerm, termInfo);

        //- Create new term info object and start adding to it
        termInfo = new TermInfo (term);
        termCount = 1;
        totalTermsInCollection++;

	//***
	//this.totalTermsInCollectionCounter.increment ();
	parameters.set ("totalTermsInCollection", totalTermsInCollection);
	
        totalTermsInDoc++;
        //uniqueTermsInDoc++;

        //positionInfo = new PositionInfo (begPos, endPos, begOffs, endOffs);
        positionInfo = new PositionInfo (begPos, begOffs);
        termInfo.positionInfoList.add (positionInfo);

        lastDocTermsInfo = docTermsInfo;
        lastTermInfo = termInfo;
        lastTerm = term;
      }
    }
    else {
      //- New document.  Save current term info terms in doc list, then serialize the
      //  current DocTermsInfo object and pass it to next step as a KeyValuePair.
      termInfo = lastTermInfo;
      termInfo.termFreq = termInfo.positionInfoList.size ();

      docTermsInfo = lastDocTermsInfo;
      docTermsInfo.termsInfoHM.put (lastTerm, termInfo);
      docTermsInfo.docUniqueTermCount = docTermsInfo.termsInfoHM.size ();
      docTermsInfo.docTermCount = totalTermsInDoc;
      docTermsInfo.docMaxTermFreq = maxTermFreqInDoc;

      //- Update some parameter values for use in determining collection stats
      parameters.set ("fwindexStatistics/totalTermsInCollection", docTermsInfo.docTermCount);
      parameters.set ("fwindexStatistics/maxTermFreqInCollection", docTermsInfo.docMaxTermFreq);

      //- Serialize the docTermsInfo object for passing as Key value pair.
      try {
        byte[] docIdBytes = Utility.fromLong (lastDocID);
        byte[] docTermsInfoBytes = ForwardIndexSerializer.toBytes (docTermsInfo);
        processor.process (new KeyValuePair (docIdBytes, docTermsInfoBytes));
      } 
      catch (IOException ioe) {
        FWINDEXREDUCELOG.severe ("Failure serializing DocTermInfo object for doc ID " + docid);
      }

      //- Reset doc stats
      //docMaxTermFreq = 0;
      termCount = 0;

      //- Create a new DocTermsInfo object for the new doc ID
      docTermsInfo = new DocTermsInfo (docid);
      docsInCollection++;

      //parameters.set ("docsInCollection", docsInCollection);
      this.tfParameters.getJSON ().set ("docsInCollection", docsInCollection);
      lastDocTermsInfo = docTermsInfo;

      //- Start a new TermInfo object for the new term
      termInfo = new TermInfo (term);
      termCount = 1;
      totalTermsInCollection++;

      parameters.set ("totalTermsInCollection", totalTermsInCollection);
      
      totalTermsInDoc = 1;
      uniqueTermsInDoc = 1;
      lastTermInfo = termInfo;

      //positionInfo = new PositionInfo (begPos, endPos, begOffs, endOffs);
      positionInfo = new PositionInfo (begPos, begOffs);
      termInfo.positionInfoList.add (positionInfo);

      lastTerm = term;
      lastDocID = docid;
    }
  }  //- end method process

}  //- end class ForwardIndexReduce

