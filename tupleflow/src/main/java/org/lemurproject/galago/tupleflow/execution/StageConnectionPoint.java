// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;
import org.lemurproject.galago.tupleflow.Order;

/**
 * Represents an endpoint for a connection within a TupleFlow stage.
 * This is defined with an input or output tag in the connections section
 * of a stage in the XML parameter file.
 * 
 * @see org.galagosearch.tupleflow.execution.Job
 * @author trevor
 */
public class StageConnectionPoint extends Locatable implements Serializable {
    public ConnectionPointType type;
    public String externalName;
    public String internalName;
    private String className;
    private String[] order;
    
    public StageConnectionPoint(ConnectionPointType type, String name, Order order) {
        super(null);
        this.type = type;
        this.externalName = name;
        this.internalName = name;
        this.className = order.getOrderedClass().getName();
        this.order = order.getOrderSpec();
    }
    
    public StageConnectionPoint(ConnectionPointType type, String name, Order order, FileLocation location) {
        super(location);
        this.type = type;
        this.externalName = name;
        this.internalName = name;
        this.className = order.getOrderedClass().getName();
        this.order = order.getOrderSpec();
    }

    public StageConnectionPoint(ConnectionPointType type, String name, String className, String[] order, FileLocation location) {
        super(location);
        this.type = type;
        this.externalName = name;
        this.internalName = name;
        this.className = className;
        this.order = order;
    }

    public StageConnectionPoint(ConnectionPointType type, String externalName, String internalName, String className, String[] order, FileLocation location) {
        super(location);
        this.type = type;
        this.externalName = externalName;
        this.internalName = internalName;
        this.className = className;
        this.order = order;
    }
    
    public String getExternalName() {
        return externalName;
    }
    
    public String getInternalName() {
        return internalName;
    }

    public ConnectionPointType getType() {
        return type;
    }

    public String getClassName() {
        return className;
    }

    public String[] getOrder() {
        return order;
    }
}
