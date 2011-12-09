// BSD License (http://lemurproject.org)

package org.lemurproject.galago.tupleflow;

import java.util.Collection;
import java.util.Comparator;

/**
 * An Order is a class that represents an ordering of a Galago Type.  You won't usually
 * implement this interface directly; instead, let Galago make the class for you
 * with the TemplateTypeBuilder/TypeBuilderMojo tools.
 * 
 * @author trevor
 * @param <T> The ordered class.
 */
public interface Order<T> {
    /// Returns the class ordered by this Order.
    public Class<T> getOrderedClass();
    /**
     * Returns a string representation of the fields ordered by this class.  For example:
     * <pre>{ "+document", "-score" }</pre>
     * means that this order orders first by the document number in ascending order, but
     * breaks ties by the score in descending order.
     */
    public String[] getOrderSpec();
    
    /**
     * Returns a comparator that applies this order to objects of type T.
     * For example, if <pre>getOrderSpec() == { "+document" }</pre> and
     * a.document = 5 and b.document = 7, then:
     * <pre>lessThan().compare(a, b) < 0</pre>. 
     */
    public Comparator<T> lessThan();
    /**
     * lessThan().compare(a,b) = greaterThan().compare(b,a);
     */
    public Comparator<T> greaterThan();
    
    /**
     * This is a hash function over an object that only uses ordered fields.
     * For instance, if the order is <pre>{ "+document", "-score" }</pre>, this
     * hash function incorporates data from the document and score fields, but
     * no other fields.
     * 
     * @param object
     * @return
     */
    public int hash(T object);
    
    /**
     * Produces an OrderedWriter object that can write objects of class T in this
     * order.  This object assumes that its input is already correctly ordered.
     * The OrderedWriter uses the ordering property to write the data in
     * compressed form.
     * 
     * @param output
     * @return
     */
    public Processor<T> orderedWriter(ArrayOutput output);
    
    /**
     * Produces an OrderedReader object.  This object can read objects that were
     * written with an OrderedWriter object produced by the Order.orderedWriter method.
     * 
     * @param input
     * @return
     */
    public TypeReader<T> orderedReader(ArrayInput input);
    
    /**
     * Produces an OrderedReader object.  This is just like the previous orderedReader
     * method, except you can explicitly set a buffer size.
     * 
     * @param input
     * @param bufferSize
     * @return
     */
    public TypeReader<T> orderedReader(ArrayInput input, int bufferSize);
    
    /**
     * Produces an OrderedCombiner object.  An ordered combiner merges objects
     * from many OrderedReaders into a single ordered stream of objects.
     * 
     * @param readers
     * @param closeOnExit
     * @return
     */
    public ReaderSource<T> orderedCombiner(Collection<TypeReader<T>> readers, boolean closeOnExit);
}
