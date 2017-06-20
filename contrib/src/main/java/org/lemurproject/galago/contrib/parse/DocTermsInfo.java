// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.io.Serializable;


/**
 * An object for storing information about terms in a specified document.
 * Includes some API calls ffor accessing object.
 *
 * @author smh
 */


public class DocTermsInfo implements Serializable {

  private static final long serialVersionUID = 7456450433890738221L;


  public long docid;

  //- Number of unique terms in doc
  public int docUniqueTermCount;

  //- number of terms in doc (total occurences) 
  public int docTermCount;

  //- Maximum frequency of any term in doc
  public int docMaxTermFreq;
      
  public HashMap<String, TermInfo> termsInfoHM;

    
  public DocTermsInfo () {
    this.docid = -1L;
    this.docUniqueTermCount = 0;
    this.docTermCount = 0;
    this.docMaxTermFreq = 0;
    this.termsInfoHM = new HashMap<>();
  }


  public DocTermsInfo (long id) {
    this.docid = id;
    this.docUniqueTermCount = 0;
    this.docTermCount = 0;
    this.docMaxTermFreq = 0;
    this.termsInfoHM = new HashMap<>();
  }


  //- Return a list of terms from for a specified document ID                                                                               
  public ArrayList<String> getDocTerms () {

    ArrayList<String>docTermsList = new ArrayList<>();
    Set keySet = this.termsInfoHM.entrySet ();
    Iterator itr = keySet.iterator ();
    while (itr.hasNext ()) {
      Map.Entry me = (Map.Entry)itr.next ();
      String key = (String)me.getKey ();
      TermInfo ti = (TermInfo)me.getValue ();
      String term = ti.term;
      docTermsList.add (term);
    }

    return docTermsList;

  }  //- end method getDocTerms


  //- Return a list of PositionInfo objects (begin and end positions and offsets)
  //  for a specified document ID and term.
  public ArrayList<PositionInfo> getDocTermPositions  (String term) {
    
    HashMap<String, TermInfo>tiHM = this.termsInfoHM;
    TermInfo ti = tiHM.get (term);

    if (ti == null) {
      return null;
    }

    return ti.positionInfoList;

  }  //-end method getDocTermPositions


  //- Get the number of terms in the specified document (total occurrences)
  public int getTermCount () {
    return this.docTermCount;  
  }


  //- Get the maximum frequency of a term in the document
  public int getMaxTermFreq () {
    return this.docMaxTermFreq;
  }


  //- Get the number of unique terms in the document
  public int getUniqueTermCount () {
    return this.docUniqueTermCount;

  }  //- end method get UniqueTermCount


  //public static String toString (TermInfo ti) {
  public static String toString (TermInfo ti) {

    StringBuffer sb = new StringBuffer ();
    sb.append ("\t Term: " + ti.term + " \t Freq: " + ti.termFreq + "\n");

    //- Term positions list
    for (PositionInfo pi : ti.positionInfoList) {
      //sb.append ("\t\t begPos: " + pi.begPos + "  endPos: " + pi.endPos +
      //           "  befOffs: " + pi.begOffs + "  endOffs: " + pi.endOffs + "\n");
      sb.append ("\t\t begPos: " + pi.begPos + "  begOffs: " + pi.begOffs + "\n");
    }

    return sb.toString ();

  }  //- end method toString


  public String printDocTermStats () {

    StringBuffer sb = new StringBuffer ();
    sb.append ("Doc ID: " + this.docid + " ==> Unique Terms: " + this.docUniqueTermCount + 
               " \t Term Count: " + this.docTermCount +
               " \t Term Max Freq: " + this.docMaxTermFreq + "\n");
  
    return sb.toString ();
  
  }  //- end method printDocTermStats


  public static class TermInfo implements Serializable {

    //- the term
    public String term;

    //- how many times the term occurs in the doc
    public int    termFreq;

    //- List of positions for a term
    public ArrayList<PositionInfo> positionInfoList; 

    public TermInfo () {
      this.term = null;
      this.termFreq = 0;
      this.positionInfoList = new ArrayList<PositionInfo>();
    }

    public TermInfo (String term) {
      this.term = term;
      this.termFreq = 0;
      this.positionInfoList = new ArrayList<PositionInfo>();
    }
  }  //- end inner class TermInfo


  public static class PositionInfo implements Serializable {
    public int begPos;
    //public int endPos;   Save space and calculate end positions as needed (just begPos+1)
    public int begOffs;
    //public int endOffs;  Save space and calculate end offsets as needed (just begOffs+term.length())

    //public PositionInfo (int bP, int eP, int bO, int eO) {
    public PositionInfo (int bP, int bO) {
      this.begPos = bP;
      //this.endPos = eP;
      this.begOffs = bO;
      //this.endOffs = eO;
    }
  }  //- end inner class PositionInfo

}  //- end class DocTermsInfo
