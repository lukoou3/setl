package com.lk.setl.sql.types;

import java.util.*;


/**
 * To get/create specific data type, users should use singleton objects and factory methods
 * provided by this class.
 *
 */
public class DataTypes {
  /**
   * Gets the StringType object.
   */
  public static final DataType StringType = StringType$.MODULE$;

  /**
   * Gets the BinaryType object.
   */
  public static final DataType BinaryType = BinaryType$.MODULE$;

  /**
   * Gets the BooleanType object.
   */
  public static final DataType BooleanType = BooleanType$.MODULE$;

  /**
   * Gets the DoubleType object.
   */
  public static final DataType DoubleType = DoubleType$.MODULE$;

  /**
   * Gets the FloatType object.
   */
  public static final DataType FloatType = FloatType$.MODULE$;

  /**
   * Gets the IntegerType object.
   */
  public static final DataType IntegerType = IntegerType$.MODULE$;

  /**
   * Gets the LongType object.
   */
  public static final DataType LongType = LongType$.MODULE$;

  /**
   * Gets the NullType object.
   */
  public static final DataType NullType = NullType$.MODULE$;

  /**
   * Creates an ArrayType by specifying the data type of elements ({@code elementType}).
   * The field of {@code containsNull} is set to {@code true}.
   */
  public static ArrayType createArrayType(DataType elementType) {
    if (elementType == null) {
      throw new IllegalArgumentException("elementType should not be null.");
    }
    return new ArrayType(elementType, true);
  }

  /**
   * Creates an ArrayType by specifying the data type of elements ({@code elementType}) and
   * whether the array contains null values ({@code containsNull}).
   */
  public static ArrayType createArrayType(DataType elementType, boolean containsNull) {
    if (elementType == null) {
      throw new IllegalArgumentException("elementType should not be null.");
    }
    return new ArrayType(elementType, containsNull);
  }

  /**
   * Creates a StructField by specifying the name ({@code name}), data type ({@code dataType}) and
   * whether values of this field can be null values ({@code nullable}).
   */
  public static StructField createStructField(
      String name,
      DataType dataType) {
    if (name == null) {
      throw new IllegalArgumentException("name should not be null.");
    }
    if (dataType == null) {
      throw new IllegalArgumentException("dataType should not be null.");
    }
    return new StructField(name, dataType);
  }


  /**
   * Creates a StructType with the given list of StructFields ({@code fields}).
   */
  public static StructType createStructType(List<StructField> fields) {
    return createStructType(fields.toArray(new StructField[fields.size()]));
  }

  /**
   * Creates a StructType with the given StructField array ({@code fields}).
   */
  public static StructType createStructType(StructField[] fields) {
    if (fields == null) {
      throw new IllegalArgumentException("fields should not be null.");
    }
    Set<String> distinctNames = new HashSet<>();
    for (StructField field : fields) {
      if (field == null) {
        throw new IllegalArgumentException(
          "fields should not contain any null.");
      }

      distinctNames.add(field.name());
    }
    if (distinctNames.size() != fields.length) {
      throw new IllegalArgumentException("fields should have distinct names.");
    }

    return StructType$.MODULE$.apply(fields);
  }
}
