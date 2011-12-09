// BSD License (http://lemurproject.org/galago-galago-license)
package org.lemurproject.galago.tupleflow.typebuilder;

import java.util.ArrayList;

/**
 *
 * @author trevor
 */
public class OrderSpecification {
    public OrderSpecification() {
        orderedFields = new ArrayList<OrderedFieldSpecification>();
    }
    
    public OrderSpecification(ArrayList<OrderedFieldSpecification> orderedFields) {
        this.orderedFields = orderedFields;
    }
    
    public ArrayList<OrderedFieldSpecification> getOrderedFields() {
        return orderedFields;
    }
    
    public void addOrderedField(OrderedFieldSpecification field) {
        orderedFields.add(field);
    }
    
    protected ArrayList<OrderedFieldSpecification> orderedFields;
}
