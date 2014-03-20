// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

/**
 * Currently represents the context that the entire query processor shares. This
 * is the most basic context we use.
 *
 * @author irmarc, sjh
 */
public class ScoringContext {

  public long document;
  // indicates when nodes can/can't cache data
  // -- useful for passage or extent retrieval.
  public boolean cachable = true;

  public ScoringContext() {
  }

  public ScoringContext(long doc) {
    this.document = doc;
  }


    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        ScoringContext that = (ScoringContext) o;

        if (document != that.document) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (document ^ (document >>> 32));
    }

    public ScoringContext getPrototype() {
        ScoringContext context = new ScoringContext();
        context.document = this.document;
        context.cachable = this.cachable;
        return context;
    }

    public void setFrom(ScoringContext other){
        assert (other != null);

        if(this.getClass() != other.getClass()){
            throw new UnsupportedOperationException("ScoringContext implementation class changed from "+
                    this.getClass()+" to "+other.getClass());
        }
        this.document = other.document;
        this.cachable = other.cachable;
    }
}
