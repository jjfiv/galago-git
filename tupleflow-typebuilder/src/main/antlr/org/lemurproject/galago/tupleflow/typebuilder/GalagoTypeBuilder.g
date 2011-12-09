         
grammar GalagoTypeBuilder;

@header {
  package org.lemurproject.galago.tupleflow.typebuilder;
  import java.util.HashMap;
  import org.lemurproject.galago.tupleflow.typebuilder.OrderSpecification;
  import org.lemurproject.galago.tupleflow.typebuilder.OrderedFieldSpecification;
  import org.lemurproject.galago.tupleflow.typebuilder.Direction;
  import org.lemurproject.galago.tupleflow.typebuilder.FieldSpecification;
}

@lexer::header {
  package org.lemurproject.galago.tupleflow.typebuilder;
}       
           
var_type returns [ FieldSpecification.DataType dataType ] :
    'bytes' { dataType = FieldSpecification.DataType.BYTES; } |
    'boolean' { dataType = FieldSpecification.DataType.BOOLEAN; } |
    'int' { dataType = FieldSpecification.DataType.INT; } |
    'long' { dataType = FieldSpecification.DataType.LONG; } |
    'short' { dataType = FieldSpecification.DataType.SHORT; } |
    'byte' { dataType = FieldSpecification.DataType.BYTE; } |
    'float' { dataType = FieldSpecification.DataType.FLOAT; } |
    'double' { dataType = FieldSpecification.DataType.DOUBLE; } |
    'String' { dataType = FieldSpecification.DataType.STRING; };
    
field_def returns [ FieldSpecification field ] :
    v=var_type i=ID ';'
    { $field = new FieldSpecification(v, $i.text); }
    ;

field_defs returns [ ArrayList<FieldSpecification> fields ] :
    { fields = new ArrayList<FieldSpecification>(); }
    (v=field_def { fields.add(v); })+
    ;   
    
order_field returns [ OrderedFieldSpecification ord_field ] :
    {Direction direction = Direction.ASCENDING;}
    ('+' | '-' {direction = Direction.DESCENDING;})
    i=ID { ord_field = new OrderedFieldSpecification(direction, $i.text); };
    
order_def returns [ OrderSpecification defs ] :
    { defs = new OrderSpecification(); }
    'order:' (o=order_field { defs.addOrderedField(o); })* ';'; 
    
order_defs returns [ ArrayList<OrderSpecification> defs ] :
    { defs = new ArrayList<OrderSpecification>(); }
    (o=order_def { defs.add(o); })+
    ;

package_name returns [ String name ] :
    ID ('.' ID)* { $package_name.name = $package_name.text; }
    ;

package_def returns [ String name ] :
    'package' pn=package_name ';' { $package_def.name = $pn.name; }
    ;      
    
type_def returns [ TypeSpecification spec ] :
    {
        spec = new TypeSpecification();
    }                                       
    p=package_def { spec.setPackageName(p); }
    'type' i=ID { spec.setTypeName($i.text); }
    '{'
    v=field_defs { spec.setFields(v); }
    o=order_defs { spec.setOrders(o); }
    '}'
    ;

ID  :   ('a'..'z'|'A'..'Z')+ ;
NEWLINE:'\r'? '\n' {skip();} ;
WS  :   (' '|'\t')+ {skip();} ;
COMMENT : '/' '/' (~('\n'|'\r'))* {skip();} ;
