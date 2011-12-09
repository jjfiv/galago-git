/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.ReaderSource;
import org.lemurproject.galago.tupleflow.ArrayOutput;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.ArrayInput;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.Type;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Processor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;   
import java.util.Comparator;
import java.util.Collection;

/**
 *
 * @author trevor
 */
public class FakeType implements Type {
    public int value;
    
    public Order<FakeType> getOrder( String... fields ) {
        return new Order<FakeType>() {
            public Class<FakeType> getOrderedClass() {
                return FakeType.class;
            }

            public String[] getOrderSpec() {
                return new String[] { "+value" };
            }
    
            public Comparator<FakeType> lessThan() {
                return new Comparator<FakeType>() {
                    public int compare(FakeType one, FakeType two) {
                        return Utility.compare(one.value, two.value);
                    }
                };
            }
            
            public Comparator<FakeType> greaterThan() {
                return new Comparator<FakeType>() {
                    public int compare(FakeType one, FakeType two) {
                        return -Utility.compare(one.value, two.value);
                    }
                };
            }
            
            public int hash( FakeType object ) {
                return object.value;
            }

            public Processor<FakeType> orderedWriter( final ArrayOutput output ) {
                return new Processor<FakeType>() {
                    public void process( FakeType object ) throws IOException {
                        output.writeInt(object.value);
                    }
                    
                    public void close() {
                    }
                };
            }
            
            public TypeReader<FakeType> orderedReader( ArrayInput input ) {
                return null;
            }
            public TypeReader<FakeType> orderedReader( ArrayInput input, int bufferSize ) {
                return orderedReader(input);
            }
            
            public ReaderSource<FakeType> orderedCombiner( Collection< TypeReader<FakeType> > readers, boolean closeOnExit ) {
                return null;
            }
        };
    }
}
