// BSD License (http://lemurproject.org/galago-galago-license)

package org.lemurproject.galago.tupleflow.typebuilder;

import java.util.ArrayList;

/**
 *
 * @author trevor
 */
public class TypeSpecification {
    public TypeSpecification() {
        this.packageName = "";
        this.typeName = "";
        this.fields = new ArrayList<FieldSpecification>();
        this.orders = new ArrayList<OrderSpecification>();
    }

    public void addFieldSpecification(FieldSpecification.DataType type, String name) {
        FieldSpecification field = new FieldSpecification(type, name);
        fields.add(field);
    }
    
    public void addOrderSpecification(OrderSpecification order) {
        orders.add(order);
    }
    
    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }
    
    public String getPackageName() {
        return this.packageName;
    }
    
    public void setFields(ArrayList<FieldSpecification> fields) {
        this.fields = fields;
    }

    public void setOrders(ArrayList<OrderSpecification> orders) {
        this.orders = orders;
    }

    public ArrayList<FieldSpecification> getFields() {
        return fields;
    }

    public ArrayList<OrderSpecification> getOrders() {
        return orders;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }
    
    String packageName;
    String typeName;
    ArrayList<FieldSpecification> fields;
    ArrayList<OrderSpecification> orders;            
}
