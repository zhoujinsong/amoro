/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.amoro.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2024-04-29")
public class SchemaUpdateMeta implements org.apache.thrift.TBase<SchemaUpdateMeta, SchemaUpdateMeta._Fields>, java.io.Serializable, Cloneable, Comparable<SchemaUpdateMeta> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SchemaUpdateMeta");

  private static final org.apache.thrift.protocol.TField SCHEMA_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("schemaId", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField UPDATE_COLUMNS_FIELD_DESC = new org.apache.thrift.protocol.TField("updateColumns", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new SchemaUpdateMetaStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new SchemaUpdateMetaTupleSchemeFactory();

  public int schemaId; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<UpdateColumn> updateColumns; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SCHEMA_ID((short)1, "schemaId"),
    UPDATE_COLUMNS((short)2, "updateColumns");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // SCHEMA_ID
          return SCHEMA_ID;
        case 2: // UPDATE_COLUMNS
          return UPDATE_COLUMNS;
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
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __SCHEMAID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SCHEMA_ID, new org.apache.thrift.meta_data.FieldMetaData("schemaId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.UPDATE_COLUMNS, new org.apache.thrift.meta_data.FieldMetaData("updateColumns", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, UpdateColumn.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SchemaUpdateMeta.class, metaDataMap);
  }

  public SchemaUpdateMeta() {
  }

  public SchemaUpdateMeta(
    int schemaId,
    java.util.List<UpdateColumn> updateColumns)
  {
    this();
    this.schemaId = schemaId;
    setSchemaIdIsSet(true);
    this.updateColumns = updateColumns;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SchemaUpdateMeta(SchemaUpdateMeta other) {
    __isset_bitfield = other.__isset_bitfield;
    this.schemaId = other.schemaId;
    if (other.isSetUpdateColumns()) {
      java.util.List<UpdateColumn> __this__updateColumns = new java.util.ArrayList<UpdateColumn>(other.updateColumns.size());
      for (UpdateColumn other_element : other.updateColumns) {
        __this__updateColumns.add(new UpdateColumn(other_element));
      }
      this.updateColumns = __this__updateColumns;
    }
  }

  public SchemaUpdateMeta deepCopy() {
    return new SchemaUpdateMeta(this);
  }

  @Override
  public void clear() {
    setSchemaIdIsSet(false);
    this.schemaId = 0;
    this.updateColumns = null;
  }

  public int getSchemaId() {
    return this.schemaId;
  }

  public SchemaUpdateMeta setSchemaId(int schemaId) {
    this.schemaId = schemaId;
    setSchemaIdIsSet(true);
    return this;
  }

  public void unsetSchemaId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SCHEMAID_ISSET_ID);
  }

  /** Returns true if field schemaId is set (has been assigned a value) and false otherwise */
  public boolean isSetSchemaId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SCHEMAID_ISSET_ID);
  }

  public void setSchemaIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SCHEMAID_ISSET_ID, value);
  }

  public int getUpdateColumnsSize() {
    return (this.updateColumns == null) ? 0 : this.updateColumns.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<UpdateColumn> getUpdateColumnsIterator() {
    return (this.updateColumns == null) ? null : this.updateColumns.iterator();
  }

  public void addToUpdateColumns(UpdateColumn elem) {
    if (this.updateColumns == null) {
      this.updateColumns = new java.util.ArrayList<UpdateColumn>();
    }
    this.updateColumns.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<UpdateColumn> getUpdateColumns() {
    return this.updateColumns;
  }

  public SchemaUpdateMeta setUpdateColumns(@org.apache.thrift.annotation.Nullable java.util.List<UpdateColumn> updateColumns) {
    this.updateColumns = updateColumns;
    return this;
  }

  public void unsetUpdateColumns() {
    this.updateColumns = null;
  }

  /** Returns true if field updateColumns is set (has been assigned a value) and false otherwise */
  public boolean isSetUpdateColumns() {
    return this.updateColumns != null;
  }

  public void setUpdateColumnsIsSet(boolean value) {
    if (!value) {
      this.updateColumns = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case SCHEMA_ID:
      if (value == null) {
        unsetSchemaId();
      } else {
        setSchemaId((java.lang.Integer)value);
      }
      break;

    case UPDATE_COLUMNS:
      if (value == null) {
        unsetUpdateColumns();
      } else {
        setUpdateColumns((java.util.List<UpdateColumn>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case SCHEMA_ID:
      return getSchemaId();

    case UPDATE_COLUMNS:
      return getUpdateColumns();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case SCHEMA_ID:
      return isSetSchemaId();
    case UPDATE_COLUMNS:
      return isSetUpdateColumns();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof SchemaUpdateMeta)
      return this.equals((SchemaUpdateMeta)that);
    return false;
  }

  public boolean equals(SchemaUpdateMeta that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_schemaId = true;
    boolean that_present_schemaId = true;
    if (this_present_schemaId || that_present_schemaId) {
      if (!(this_present_schemaId && that_present_schemaId))
        return false;
      if (this.schemaId != that.schemaId)
        return false;
    }

    boolean this_present_updateColumns = true && this.isSetUpdateColumns();
    boolean that_present_updateColumns = true && that.isSetUpdateColumns();
    if (this_present_updateColumns || that_present_updateColumns) {
      if (!(this_present_updateColumns && that_present_updateColumns))
        return false;
      if (!this.updateColumns.equals(that.updateColumns))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + schemaId;

    hashCode = hashCode * 8191 + ((isSetUpdateColumns()) ? 131071 : 524287);
    if (isSetUpdateColumns())
      hashCode = hashCode * 8191 + updateColumns.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(SchemaUpdateMeta other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetSchemaId()).compareTo(other.isSetSchemaId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSchemaId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.schemaId, other.schemaId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetUpdateColumns()).compareTo(other.isSetUpdateColumns());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUpdateColumns()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.updateColumns, other.updateColumns);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("SchemaUpdateMeta(");
    boolean first = true;

    sb.append("schemaId:");
    sb.append(this.schemaId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("updateColumns:");
    if (this.updateColumns == null) {
      sb.append("null");
    } else {
      sb.append(this.updateColumns);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SchemaUpdateMetaStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public SchemaUpdateMetaStandardScheme getScheme() {
      return new SchemaUpdateMetaStandardScheme();
    }
  }

  private static class SchemaUpdateMetaStandardScheme extends org.apache.thrift.scheme.StandardScheme<SchemaUpdateMeta> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SchemaUpdateMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SCHEMA_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.schemaId = iprot.readI32();
              struct.setSchemaIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // UPDATE_COLUMNS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list82 = iprot.readListBegin();
                struct.updateColumns = new java.util.ArrayList<UpdateColumn>(_list82.size);
                @org.apache.thrift.annotation.Nullable UpdateColumn _elem83;
                for (int _i84 = 0; _i84 < _list82.size; ++_i84)
                {
                  _elem83 = new UpdateColumn();
                  _elem83.read(iprot);
                  struct.updateColumns.add(_elem83);
                }
                iprot.readListEnd();
              }
              struct.setUpdateColumnsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, SchemaUpdateMeta struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(SCHEMA_ID_FIELD_DESC);
      oprot.writeI32(struct.schemaId);
      oprot.writeFieldEnd();
      if (struct.updateColumns != null) {
        oprot.writeFieldBegin(UPDATE_COLUMNS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.updateColumns.size()));
          for (UpdateColumn _iter85 : struct.updateColumns)
          {
            _iter85.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SchemaUpdateMetaTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public SchemaUpdateMetaTupleScheme getScheme() {
      return new SchemaUpdateMetaTupleScheme();
    }
  }

  private static class SchemaUpdateMetaTupleScheme extends org.apache.thrift.scheme.TupleScheme<SchemaUpdateMeta> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SchemaUpdateMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetSchemaId()) {
        optionals.set(0);
      }
      if (struct.isSetUpdateColumns()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetSchemaId()) {
        oprot.writeI32(struct.schemaId);
      }
      if (struct.isSetUpdateColumns()) {
        {
          oprot.writeI32(struct.updateColumns.size());
          for (UpdateColumn _iter86 : struct.updateColumns)
          {
            _iter86.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SchemaUpdateMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.schemaId = iprot.readI32();
        struct.setSchemaIdIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list87 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.updateColumns = new java.util.ArrayList<UpdateColumn>(_list87.size);
          @org.apache.thrift.annotation.Nullable UpdateColumn _elem88;
          for (int _i89 = 0; _i89 < _list87.size; ++_i89)
          {
            _elem88 = new UpdateColumn();
            _elem88.read(iprot);
            struct.updateColumns.add(_elem88);
          }
        }
        struct.setUpdateColumnsIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

