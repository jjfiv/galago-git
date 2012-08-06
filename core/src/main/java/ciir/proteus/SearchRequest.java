/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package ciir.proteus;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchRequest implements org.apache.thrift.TBase<SearchRequest, SearchRequest._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SearchRequest");

  private static final org.apache.thrift.protocol.TField RAW_QUERY_FIELD_DESC = new org.apache.thrift.protocol.TField("raw_query", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TYPES_FIELD_DESC = new org.apache.thrift.protocol.TField("types", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField PARAMETERS_FIELD_DESC = new org.apache.thrift.protocol.TField("parameters", org.apache.thrift.protocol.TType.STRUCT, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SearchRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SearchRequestTupleSchemeFactory());
  }

  public String raw_query; // required
  public List<ProteusType> types; // required
  public RequestParameters parameters; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    RAW_QUERY((short)1, "raw_query"),
    TYPES((short)2, "types"),
    PARAMETERS((short)3, "parameters");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // RAW_QUERY
          return RAW_QUERY;
        case 2: // TYPES
          return TYPES;
        case 3: // PARAMETERS
          return PARAMETERS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private _Fields optionals[] = {_Fields.PARAMETERS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.RAW_QUERY, new org.apache.thrift.meta_data.FieldMetaData("raw_query", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TYPES, new org.apache.thrift.meta_data.FieldMetaData("types", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, ProteusType.class))));
    tmpMap.put(_Fields.PARAMETERS, new org.apache.thrift.meta_data.FieldMetaData("parameters", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RequestParameters.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SearchRequest.class, metaDataMap);
  }

  public SearchRequest() {
  }

  public SearchRequest(
    String raw_query,
    List<ProteusType> types)
  {
    this();
    this.raw_query = raw_query;
    this.types = types;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SearchRequest(SearchRequest other) {
    if (other.isSetRaw_query()) {
      this.raw_query = other.raw_query;
    }
    if (other.isSetTypes()) {
      List<ProteusType> __this__types = new ArrayList<ProteusType>();
      for (ProteusType other_element : other.types) {
        __this__types.add(other_element);
      }
      this.types = __this__types;
    }
    if (other.isSetParameters()) {
      this.parameters = new RequestParameters(other.parameters);
    }
  }

  public SearchRequest deepCopy() {
    return new SearchRequest(this);
  }

  @Override
  public void clear() {
    this.raw_query = null;
    this.types = null;
    this.parameters = null;
  }

  public String getRaw_query() {
    return this.raw_query;
  }

  public SearchRequest setRaw_query(String raw_query) {
    this.raw_query = raw_query;
    return this;
  }

  public void unsetRaw_query() {
    this.raw_query = null;
  }

  /** Returns true if field raw_query is set (has been assigned a value) and false otherwise */
  public boolean isSetRaw_query() {
    return this.raw_query != null;
  }

  public void setRaw_queryIsSet(boolean value) {
    if (!value) {
      this.raw_query = null;
    }
  }

  public int getTypesSize() {
    return (this.types == null) ? 0 : this.types.size();
  }

  public java.util.Iterator<ProteusType> getTypesIterator() {
    return (this.types == null) ? null : this.types.iterator();
  }

  public void addToTypes(ProteusType elem) {
    if (this.types == null) {
      this.types = new ArrayList<ProteusType>();
    }
    this.types.add(elem);
  }

  public List<ProteusType> getTypes() {
    return this.types;
  }

  public SearchRequest setTypes(List<ProteusType> types) {
    this.types = types;
    return this;
  }

  public void unsetTypes() {
    this.types = null;
  }

  /** Returns true if field types is set (has been assigned a value) and false otherwise */
  public boolean isSetTypes() {
    return this.types != null;
  }

  public void setTypesIsSet(boolean value) {
    if (!value) {
      this.types = null;
    }
  }

  public RequestParameters getParameters() {
    return this.parameters;
  }

  public SearchRequest setParameters(RequestParameters parameters) {
    this.parameters = parameters;
    return this;
  }

  public void unsetParameters() {
    this.parameters = null;
  }

  /** Returns true if field parameters is set (has been assigned a value) and false otherwise */
  public boolean isSetParameters() {
    return this.parameters != null;
  }

  public void setParametersIsSet(boolean value) {
    if (!value) {
      this.parameters = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case RAW_QUERY:
      if (value == null) {
        unsetRaw_query();
      } else {
        setRaw_query((String)value);
      }
      break;

    case TYPES:
      if (value == null) {
        unsetTypes();
      } else {
        setTypes((List<ProteusType>)value);
      }
      break;

    case PARAMETERS:
      if (value == null) {
        unsetParameters();
      } else {
        setParameters((RequestParameters)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case RAW_QUERY:
      return getRaw_query();

    case TYPES:
      return getTypes();

    case PARAMETERS:
      return getParameters();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case RAW_QUERY:
      return isSetRaw_query();
    case TYPES:
      return isSetTypes();
    case PARAMETERS:
      return isSetParameters();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SearchRequest)
      return this.equals((SearchRequest)that);
    return false;
  }

  public boolean equals(SearchRequest that) {
    if (that == null)
      return false;

    boolean this_present_raw_query = true && this.isSetRaw_query();
    boolean that_present_raw_query = true && that.isSetRaw_query();
    if (this_present_raw_query || that_present_raw_query) {
      if (!(this_present_raw_query && that_present_raw_query))
        return false;
      if (!this.raw_query.equals(that.raw_query))
        return false;
    }

    boolean this_present_types = true && this.isSetTypes();
    boolean that_present_types = true && that.isSetTypes();
    if (this_present_types || that_present_types) {
      if (!(this_present_types && that_present_types))
        return false;
      if (!this.types.equals(that.types))
        return false;
    }

    boolean this_present_parameters = true && this.isSetParameters();
    boolean that_present_parameters = true && that.isSetParameters();
    if (this_present_parameters || that_present_parameters) {
      if (!(this_present_parameters && that_present_parameters))
        return false;
      if (!this.parameters.equals(that.parameters))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(SearchRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    SearchRequest typedOther = (SearchRequest)other;

    lastComparison = Boolean.valueOf(isSetRaw_query()).compareTo(typedOther.isSetRaw_query());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRaw_query()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.raw_query, typedOther.raw_query);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTypes()).compareTo(typedOther.isSetTypes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTypes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.types, typedOther.types);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetParameters()).compareTo(typedOther.isSetParameters());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetParameters()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.parameters, typedOther.parameters);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SearchRequest(");
    boolean first = true;

    sb.append("raw_query:");
    if (this.raw_query == null) {
      sb.append("null");
    } else {
      sb.append(this.raw_query);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("types:");
    if (this.types == null) {
      sb.append("null");
    } else {
      sb.append(this.types);
    }
    first = false;
    if (isSetParameters()) {
      if (!first) sb.append(", ");
      sb.append("parameters:");
      if (this.parameters == null) {
        sb.append("null");
      } else {
        sb.append(this.parameters);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SearchRequestStandardSchemeFactory implements SchemeFactory {
    public SearchRequestStandardScheme getScheme() {
      return new SearchRequestStandardScheme();
    }
  }

  private static class SearchRequestStandardScheme extends StandardScheme<SearchRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SearchRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // RAW_QUERY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.raw_query = iprot.readString();
              struct.setRaw_queryIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TYPES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list16 = iprot.readListBegin();
                struct.types = new ArrayList<ProteusType>(_list16.size);
                for (int _i17 = 0; _i17 < _list16.size; ++_i17)
                {
                  ProteusType _elem18; // optional
                  _elem18 = ProteusType.findByValue(iprot.readI32());
                  struct.types.add(_elem18);
                }
                iprot.readListEnd();
              }
              struct.setTypesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // PARAMETERS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.parameters = new RequestParameters();
              struct.parameters.read(iprot);
              struct.setParametersIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, SearchRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.raw_query != null) {
        oprot.writeFieldBegin(RAW_QUERY_FIELD_DESC);
        oprot.writeString(struct.raw_query);
        oprot.writeFieldEnd();
      }
      if (struct.types != null) {
        oprot.writeFieldBegin(TYPES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, struct.types.size()));
          for (ProteusType _iter19 : struct.types)
          {
            oprot.writeI32(_iter19.getValue());
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.parameters != null) {
        if (struct.isSetParameters()) {
          oprot.writeFieldBegin(PARAMETERS_FIELD_DESC);
          struct.parameters.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SearchRequestTupleSchemeFactory implements SchemeFactory {
    public SearchRequestTupleScheme getScheme() {
      return new SearchRequestTupleScheme();
    }
  }

  private static class SearchRequestTupleScheme extends TupleScheme<SearchRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SearchRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetRaw_query()) {
        optionals.set(0);
      }
      if (struct.isSetTypes()) {
        optionals.set(1);
      }
      if (struct.isSetParameters()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetRaw_query()) {
        oprot.writeString(struct.raw_query);
      }
      if (struct.isSetTypes()) {
        {
          oprot.writeI32(struct.types.size());
          for (ProteusType _iter20 : struct.types)
          {
            oprot.writeI32(_iter20.getValue());
          }
        }
      }
      if (struct.isSetParameters()) {
        struct.parameters.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SearchRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.raw_query = iprot.readString();
        struct.setRaw_queryIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list21 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, iprot.readI32());
          struct.types = new ArrayList<ProteusType>(_list21.size);
          for (int _i22 = 0; _i22 < _list21.size; ++_i22)
          {
            ProteusType _elem23; // optional
            _elem23 = ProteusType.findByValue(iprot.readI32());
            struct.types.add(_elem23);
          }
        }
        struct.setTypesIsSet(true);
      }
      if (incoming.get(2)) {
        struct.parameters = new RequestParameters();
        struct.parameters.read(iprot);
        struct.setParametersIsSet(true);
      }
    }
  }

}

