// BSD License (http://www.lemurproject.org/galago-galago-license)

package org.lemurproject.galago.tupleflow.typebuilder;

/**
 *
 * @author trevor
 */
public class FieldSpecification {
    public enum DataType {
        BOOLEAN ("boolean", "boolean", "Boolean","Boolean", false, false, false),
        BYTE   ("byte", "byte", "Byte","Byte", true, false, false),
        SHORT  ("short", "short", "Short","Short", true, false, false),
        INT    ("int", "int", "Integer","Int", true, false, false),
        LONG   ("long", "long", "Long","Long", true, false, false),
        FLOAT  ("float", "float", "Float","Float", false, false, false),
        DOUBLE ("double", "double", "Double","Double", false, false, false),
        STRING ("String", "String", "String","String", false, true, false),
        BYTES  ("bytes", "byte", "byte[]","byte[]", false, false, true);
                
        DataType(String internalType, String baseType, String className, String boxedName,
                 boolean isInteger, boolean isString, boolean isArray) {
            this.internalType = internalType;
            this.baseType = baseType;
            this.className = className;
            this.boxedName = boxedName;
            this.isInteger = isInteger;
            this.isString = isString;
            this.isArray = isArray;
        }

        public String getType() {
            if (!isArray)
                return baseType;
            return baseType + "[]";
        }
        
        public String getBaseType() {
            return baseType;
        }
        
        public String getInternalType() {
            return internalType;
        }

        public boolean isInteger() {
            return isInteger;
        }

        public boolean isString() {
            return isString;
        }
        
        public boolean isArray() {
            return isArray;
        }

        public String getClassName() {
            return className;
        }

        public String getBoxedName() {
            return boxedName;
        }

        private final String baseType;
        private final String internalType;
        private final String className;
        private final String boxedName;
        private final boolean isInteger;
        private final boolean isString;
        private final boolean isArray;
    }
    
    public FieldSpecification(DataType type, String name) {
        this.type = type;
        this.name = name;
    }
    
    public DataType getType() {
        return type;
    }
    
    public String getName() {
        return name;
    }
    
    protected DataType type;
    protected String name;
}
