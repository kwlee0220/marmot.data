package marmot.avro;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.Date;
import org.apache.avro.LogicalTypes.TimeMillis;
import org.apache.avro.LogicalTypes.TimestampMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import utils.LocalDateTimes;
import utils.Throwables;
import utils.Tuple;
import utils.Utilities;
import utils.io.IOUtils;
import utils.stream.FStream;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.support.DataUtils;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import marmot.type.PrimitiveDataType;
import marmot.type.TypeClass;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class AvroUtils {
	private AvroUtils() {
		throw new AssertionError("should not be called: class=" + getClass());
	}
	
	public static Schema toSchema(final RecordSchema schema) {
		Utilities.checkNotNullArgument(schema);
		
		List<Field> fields = Lists.newArrayList();
		for ( Column col: schema.streamColumns() ) {
			DataType dtype = col.type();
			if ( dtype.isPrimitiveType() ) {
				Schema colSchema = PRIMITIVES.get(((PrimitiveDataType)dtype).typeClass());
				if ( colSchema == null ) {
//					if ( dtype.typeClass() == TypeClass.COORDINATE ) {
//						Schema elmSchema = Schema.create(Schema.Type.DOUBLE);
//						Schema fieldSchema = Schema.createArray(elmSchema);
//						fieldSchema.addProp("specific", TypeClass.COORDINATE.name());
//						colSchema = Schema.createUnion(fieldSchema, Schema.create(Type.NULL));
//					}
//					else if ( dtype.typeClass() == TypeClass.ENVELOPE ) {
//						Schema elmSchema = Schema.create(Schema.Type.DOUBLE);
//						Schema fieldSchema = Schema.createArray(elmSchema);
//						fieldSchema.addProp("specific", TypeClass.ENVELOPE.name());
//						colSchema = Schema.createUnion(fieldSchema, Schema.create(Type.NULL));
//					}
//					else {
						throw new IllegalArgumentException("unsupported field type for avro: column=" + col);
//					}
				}
				
				fields.add(new Field(col.name(), colSchema));
			}
			else if ( dtype.isGeometryType() ) {
				GeometryDataType geomType = (GeometryDataType)dtype;

				Schema fieldSchema = Schema.create(Type.BYTES);
				fieldSchema.addProp("specific", dtype.typeClass().name());
				fieldSchema.addProp("srid", geomType.srid());
				fieldSchema = Schema.createUnion(fieldSchema, Schema.create(Type.NULL));
				fields.add(new Field(col.name(), fieldSchema));
			}
			else {
				throw new AssertionError();
			}
		}
		
		return Schema.createRecord("simple_feature", null, "etri.marmot", false, fields);
	}
	
	public static RecordSchema toRecordSchema(Schema schema) {
		return FStream.from(schema.getFields())
						.map(field -> new Column(field.name(), toColumnDataType(field.schema())))
						.fold(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	private static DataType toColumnDataType(Schema schema) {
		if ( schema.isUnion() ) {
			schema = schema.getTypes().get(0);
		}
		
		String specific;
		switch ( schema.getType() ) {
			case STRING:
				return DataType.STRING;
			case DOUBLE:
				return DataType.DOUBLE;
			case INT:
				TypeClass tc = TypeClass.valueOf(schema.getProp("specific").toUpperCase());
				return DataType.fromTypeCode(tc);
			case LONG:
				LogicalType ltype = schema.getLogicalType();
				if ( ltype != null ) {
					if ( ltype instanceof TimestampMillis ) {
						return DataType.DATETIME;
					}
					else if ( ltype instanceof Date ) {
						return DataType.DATE;
					}
					else if ( ltype instanceof TimeMillis ) {
						return DataType.TIME;
					}
					else {
						throw new IllegalArgumentException("unexpected field type: " + schema);
					}
				}
				else {
					return DataType.LONG;
				}
			case RECORD:
				List<Field> fields = schema.getFields();
				DataType type = parseRecordSchemaName(schema.getName());
				if ( type.isGeometryType() ) {
					return type;
				}
				else if ( type.isRecordType() ) {
					throw new AssertionError("##### " + AvroUtils.class);
				}
			case BOOLEAN:
				return DataType.BOOLEAN;
			case FLOAT:
				return DataType.FLOAT;
			case BYTES:
				specific = schema.getProp("specific");
				if ( specific == null ) {
					return DataType.BINARY;
				};
				String srid = schema.getProp("srid");
				if ( srid != null ) {
					GeometryDataType gtype = (GeometryDataType)DataType.fromTypeCodeName(specific);
					return gtype.duplicate(srid);
				}
				throw new IllegalArgumentException("unexpected binary type: specific=" + specific);
			case ARRAY:
				specific = schema.getProp("specific");
				switch ( TypeClass.valueOf(specific) ) {
					case ENVELOPE:
						return DataType.ENVELOPE;
					case COORDINATE:
						return DataType.COORDINATE;
					default:
						throw new IllegalArgumentException("unexpected array-field type: " + schema);
				}
			default:
				throw new IllegalArgumentException("unexpected field type: " + schema);
		}
	}
	
	public static Record toRecord(GenericRecord grec, RecordSchema schema) {
		return new AvroRecord(schema, grec);
	}
	
	public static void copyToRecord(GenericRecord grec, Record record) {
		for ( Column col: record.getRecordSchema().getColumns() ) {
			int idx = col.ordinal();
			record.set(idx, fromAvroValue(col.type(), grec.get(idx)));
		}
	}
	
	public static void copyToGenericRecord(Record record, GenericRecord grec) {
		for ( Column col: record.getRecordSchema().getColumns() ) {
			Object obj = toAvroValue(col.type(), record.get(col.ordinal()));
			grec.put(col.ordinal(), obj);
		}
	}
	
	public static GenericRecord toGenericRecord(Record src, Schema avroSchema) {
		if ( src instanceof AvroRecord ) {
			return ((AvroRecord)src).getGenericRecord();
		}
		
		RecordSchema rschema = src.getRecordSchema();
		GenericData.Record grec = new GenericData.Record(avroSchema);
		for ( int i =0; i < rschema.getColumnCount(); ++i ) {
			Object avroObj = toAvroValue(rschema.getColumnAt(i).type(), src.get(i));
			grec.put(i, avroObj);
		}
		
		return grec;
	}
	
	static Object toAvroValue(DataType type, Object value) {
		if ( value == null ) {
			return null;
		}
		if ( type.isGeometryType() ) {
			return ByteBuffer.wrap(GeometryDataType.toWkb((Geometry)value));
		}
		
		if ( type instanceof PrimitiveDataType ) {
			switch ( type.typeClass() ) {
				case STRING:
				case INT:
				case DOUBLE:
				case FLOAT:
				case LONG:
				case BOOLEAN:
					return value;
				case BINARY:
					if ( value instanceof byte[] ) {
						return ByteBuffer.wrap((byte[])value);
					}
					else if ( value instanceof ByteBuffer) {
						return value;
					}
					throw new IllegalArgumentException("illegal value for avro binary type: value=" + value.getClass());
				case SHORT:
					return ((Short)value).intValue();
				case BYTE:
					return ((Byte)value).intValue();
				case ENVELOPE:
					Envelope envl = (Envelope)value;
					GenericData.Array<Double> envlArray = new GenericData.Array<Double>(4, SCHEMA_ENVELOPE);
					envlArray.add(envl.getMinX());
					envlArray.add(envl.getMaxX());
					envlArray.add(envl.getMinY());
					envlArray.add(envl.getMaxY());
					return envlArray;
				case COORDINATE:
					Coordinate coord = (Coordinate)value;
					GenericData.Array<Double> coordArray = new GenericData.Array<Double>(2, SCHEMA_COORDINATE);
					coordArray.add(coord.x);
					coordArray.add(coord.y);
					return coordArray;
				case DATETIME:
					return LocalDateTimes.toEpochMillis(DataUtils.asDatetime(value));
				default: 
					throw new AssertionError();
			}
		}
		else {
			throw new AssertionError();
		}
	}
	
	public static Object fromAvroValue(DataType type, Object value) {
		if ( value == null ) {
			return null;
		}
		if ( type.isGeometryType() ) {
			ByteBuffer wkb = (ByteBuffer)value;
			return GeometryDataType.fromWkb(wkb.array());
		}

		if ( type instanceof PrimitiveDataType ) {
			switch ( ((PrimitiveDataType)type).typeClass() ) {
				case STRING:
					return ((Utf8)value).toString();
				case INT:
				case DOUBLE:
				case FLOAT:
				case LONG:
				case BINARY:
				case BOOLEAN:
					return value;
				case SHORT:
					return (short)value;
				case BYTE:
					return (byte)value;
				case ENVELOPE:
					GenericArray<Double> envlArr = (GenericArray<Double>)value;
					return new Envelope(envlArr.get(0), envlArr.get(1), envlArr.get(2), envlArr.get(3));
				case COORDINATE:
					GenericArray<Double> coordArr = (GenericArray<Double>)value;
					return new Coordinate(coordArr.get(0), coordArr.get(1));
				case DATETIME:
					return LocalDateTimes.fromEpochMillis((Long)value);
				default: 
					throw new AssertionError();
			}
		}
		else {
			throw new AssertionError();
		}
	}
	
	private static Schema toGeometrySchema(GeometryDataType geomType) {
		String name = geomType.typeClass().name() + "_" + GeometryDataType.getEpsgCode(geomType.srid());
		return SchemaBuilder.record(name)
							.fields()
								.name("wkb").type().bytesType().noDefault()
							.endRecord();
	}
	
	private static DataType parseRecordSchemaName(String name) {
		if ( name.equals("record") ) {
			return null;
		}
		else {
			int idx = name.lastIndexOf('_');
			String tcName = name.substring(0, idx);
			GeometryDataType geomType = (GeometryDataType)DataType.fromTypeCodeName(tcName);
			
			String epsgCode = name.substring(idx+1);
			if ( !epsgCode.equals("?") ) {
				return geomType.duplicate("EPSG:" + epsgCode);
			}
			else {
				return geomType;
			}
		}
	}
	
	private static Schema createNullableFieldSchema(Schema.Type type, String specific) {
		Schema schema = Schema.create(type);
		if ( specific != null ) {
			schema.addProp("specific", specific);
		}
		return Schema.createUnion(schema, Schema.create(Type.NULL));
	}
	
	private static final Map<TypeClass, Schema> PRIMITIVES = Maps.newHashMap();
	private static final Schema SCHEMA_ENVELOPE;
	private static final Schema SCHEMA_COORDINATE;
	static {
		PRIMITIVES.put(TypeClass.STRING, 
						Schema.createUnion(Schema.create(Type.STRING), Schema.create(Type.NULL)));
		PRIMITIVES.put(TypeClass.INT, createNullableFieldSchema(Type.INT, TypeClass.INT.name()));
		PRIMITIVES.put(TypeClass.SHORT, createNullableFieldSchema(Type.INT, TypeClass.SHORT.name()));
		PRIMITIVES.put(TypeClass.BYTE, createNullableFieldSchema(Type.INT, TypeClass.BYTE.name()));
		PRIMITIVES.put(TypeClass.LONG, createNullableFieldSchema(Type.LONG, TypeClass.LONG.name()));
		PRIMITIVES.put(TypeClass.DOUBLE, createNullableFieldSchema(Type.DOUBLE, null));
		PRIMITIVES.put(TypeClass.FLOAT, createNullableFieldSchema(Type.FLOAT, null));
		PRIMITIVES.put(TypeClass.BOOLEAN, createNullableFieldSchema(Type.BOOLEAN, null));
		PRIMITIVES.put(TypeClass.BINARY, createNullableFieldSchema(Type.BYTES, null));
		
		PRIMITIVES.put(TypeClass.DATETIME, createNullableFieldSchema(Type.LONG, TypeClass.DATETIME.name()));
		PRIMITIVES.put(TypeClass.TIME, createNullableFieldSchema(Type.LONG, TypeClass.TIME.name()));
		PRIMITIVES.put(TypeClass.DATE, createNullableFieldSchema(Type.LONG, TypeClass.DATE.name()));
		
		SCHEMA_COORDINATE = Schema.createArray(Schema.create(Schema.Type.DOUBLE));
		SCHEMA_COORDINATE.addProp("specific", TypeClass.COORDINATE.name());
		PRIMITIVES.put(TypeClass.COORDINATE, Schema.createUnion(SCHEMA_COORDINATE, Schema.create(Type.NULL)));
		
		SCHEMA_ENVELOPE = Schema.createArray(Schema.create(Schema.Type.DOUBLE));
		SCHEMA_ENVELOPE.addProp("specific", TypeClass.ENVELOPE.name());
		PRIMITIVES.put(TypeClass.ENVELOPE, Schema.createUnion(SCHEMA_ENVELOPE, Schema.create(Type.NULL)));
	}
	
	private static final int DEFAULT_PIPE_SIZE = 128 * 1024;
	public static InputStream toSerializedInputStream(Schema avroSchema, RecordStream stream) {
		Tuple<PipedOutputStream,PipedInputStream> pipe = IOUtils.pipe(DEFAULT_PIPE_SIZE);
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			try ( OutputStream os = pipe._1 ) {
				new AvroBinaryRecordWriter(stream.getRecordSchema(), avroSchema, os).write(stream);
			}
			catch ( Exception e ) {
				Throwables.sneakyThrow(e);
			}
		});
		return pipe._2;
	}
	
	public static InputStream toSerializedInputStream(AvroRecordReader ds) {
		return toSerializedInputStream(ds.getAvroSchema(), ds.read());
	}
	
	public static InputStream toSerializedInputStream(RecordStream stream) {
		Schema avroSchema = AvroUtils.toSchema(stream.getRecordSchema());
		return toSerializedInputStream(avroSchema, stream);
	}
}
