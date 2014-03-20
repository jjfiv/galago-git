// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.processing;

/**
 * The context used for passage retrieval.
 *
 * @author irmarc
 */
public class PassageScoringContext extends ScoringContext {
  public PassageScoringContext() {
    super();
    begin = 0;
    end = Integer.MAX_VALUE;
  }
  public int begin;
  public int end;

  @Override
  public String toString(){
    return String.format("context: doc = %d, begin = %d, end = %d", document, begin, end);
  }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        PassageScoringContext that = (PassageScoringContext) o;

        if (document != that.document) return false;
        if (begin != that.begin) return false;
        if (end != that.end) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + begin;
        result = 31 * result + end;
        result = 31 * result + (int) (document ^ (document >>> 32));
        return result;
    }


    public ScoringContext getPrototype() {
        PassageScoringContext context = new PassageScoringContext();
        context.document = this.document;
        context.cachable = this.cachable;
        context.begin = this.begin;
        context.end = this.end;
        return context;
    }

    public void setFrom(ScoringContext o){
        assert (o != null);

        PassageScoringContext other = (PassageScoringContext) o;

        if(this.getClass() != other.getClass()){
            throw new UnsupportedOperationException("ScoringContext implementation class changed from "+
                    this.getClass()+" to "+other.getClass());
        }


        this.document = other.document;
        this.cachable = other.cachable;
        this.begin = other.begin;
        this.end = other.end;
    }

}
