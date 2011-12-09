// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow;

import java.io.EOFException;
import java.io.IOException;

public abstract class OrderedReader<T> implements TypeReader<T>, ExNihiloSource<T> {
    protected Processor<T> processor;
    protected ArrayInput input;
    protected boolean needsInit;
    protected T last = null;
    protected int batchCount = 0;
    
    public OrderedReader(ArrayInput input, Processor<T> processor) {
        this.input = input;
        this.processor = processor;
        needsInit = true;
        last = clone(null);
    }
    
    public OrderedReader(ArrayInput input) {
        this(input, null);
    }
    
    public void setProcessor(Processor<T> processor) {
        this.processor = processor;
    }

    public void run() throws IOException {
        T object = null;
        while ((object = read()) != null) {
            processor.process(object);
        }
        processor.close();
    }
    
    public T read() throws IOException {
        try {
            readTuple();
        } catch(EOFException e) {
            return null;
        }
        
        return clone(last);
    }
    
    public void readTuple() throws IOException {}
    public abstract T clone(T object);
}
