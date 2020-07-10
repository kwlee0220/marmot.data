package marmot.avro;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Date;
import org.apache.avro.LogicalTypes.TimeMillis;
import org.apache.avro.LogicalTypes.TimestampMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Geometry;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.support.DataUtils;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import marmot.type.PrimitiveDataType;
import marmot.type.TypeClass;
import utils.LocalDateTimes;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.io.InputStreamFromOutputStream;
import utils.stream.FStream;

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
					throw new IllegalArgumentException("unsupported field type: column=" + col);
				}
				
				fields.add(new Field(col.name(), colSchema));
			}
			else if ( dtype.isGeometryType() ) {
				GeometryDataType geomType = (GeometryDataType)dtype;
				Schema colSchema = toGeometrySchema(geomType);
				fields.add(new Field(col.name(), colSchema));
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
						.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	private static DataType toColumnDataType(Schema schema) {
		switch ( schema.getType() ) {
			case STRING:
				return DataType.STRING;
			case DOUBLE:
				return DataType.DOUBLE;
			case INT:
				return DataType.INT;
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
				return DataType.BINARY;
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
		if ( type.isGeometryType() ) {
			Schema geomSchema = toGeometrySchema((GeometryDataType)type);
			GenericRecord rec = new GenericData.Record(geomSchema);
			byte[] wkb = GeometryDataType.toWkb((Geometry)value);
			rec.put(0, ByteBuffer.wrap(wkb));
			
			return rec;
		}
		
		if ( type instanceof PrimitiveDataType ) {
			switch ( type.typeClass() ) {
				case STRING:
				case INT:
				case DOUBLE:
				case FLOAT:
				case LONG:
				case SHORT:
				case BYTE:
				case BINARY:
				case BOOLEAN:
					return value;
				case ENVELOPE:
					throw new AssertionError();
				case COORDINATE:
					throw new AssertionError();
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
	
	static Object fromAvroValue(DataType type, Object value) {
		if ( type.isGeometryType() ) {
			ByteBuffer wkb = (ByteBuffer)((GenericRecord)value).get(0);
			return GeometryDataType.fromWkb(wkb.array());
		}

		if ( type instanceof PrimitiveDataType ) {
			switch ( ((PrimitiveDataType)type).typeClass() ) {
				case STRING:
				case INT:
				case DOUBLE:
				case FLOAT:
				case LONG:
				case SHORT:
				case BYTE:
				case BINARY:
				case BOOLEAN:
					return value;
				case ENVELOPE:
					throw new AssertionError();
				case COORDINATE:
					throw new AssertionError();
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
	
	private static final Map<TypeClass, Schema> PRIMITIVES = Maps.newHashMap();
	static {
		PRIMITIVES.put(TypeClass.STRING, Schema.create(Type.STRING));
		PRIMITIVES.put(TypeClass.INT, Schema.create(Type.INT));
		PRIMITIVES.put(TypeClass.LONG, Schema.create(Type.LONG));
		PRIMITIVES.put(TypeClass.DOUBLE, Schema.create(Type.DOUBLE));
		PRIMITIVES.put(TypeClass.FLOAT, Schema.create(Type.FLOAT));
		PRIMITIVES.put(TypeClass.BOOLEAN, Schema.create(Type.BOOLEAN));
		PRIMITIVES.put(TypeClass.BINARY, Schema.create(Type.BYTES));
		
		PRIMITIVES.put(TypeClass.DATETIME, LogicalTypes.timestampMillis()
														.addToSchema(Schema.create(Schema.Type.LONG)));
		PRIMITIVES.put(TypeClass.TIME, LogicalTypes.timeMillis()
													.addToSchema(Schema.create(Schema.Type.INT)));
		PRIMITIVES.put(TypeClass.DATE, LogicalTypes.date()
													.addToSchema(Schema.create(Schema.Type.INT)));
	}
	
	private static final int DEFAULT_PIPE_SIZE = 4 * 1024 * 1024;
	public static InputStream toSerializedInputStream(Schema avroSchema, RecordStream stream) {
		return new InputStreamFromOutputStream(os -> {
			WriteRecordSetToOutStream pump = new WriteRecordSetToOutStream(stream, avroSchema, os);
			pump.start();
			return pump;
		}, DEFAULT_PIPE_SIZE);
	}
	
	public static InputStream readSerializedStream(AvroRecordReader ds) {
		return toSerializedInputStream(ds.getAvroSchema(), ds.read());
	}
	
	public static InputStream toSerializedInputStream(RecordStream stream) {
		Schema avroSchema = AvroUtils.toSchema(stream.getRecordSchema());
		return toSerializedInputStream(avroSchema, stream);
	}
	
	private static class WriteRecordSetToOutStream extends AbstractThreadedExecution<Void> {
		private final RecordStream m_stream;
		private final AvroSerializer m_writer;
		
		private WriteRecordSetToOutStream(RecordStream stream, Schema avroSchema, OutputStream os) {
			m_stream = stream;
			m_writer = new AvroSerializer(avroSchema, os);
			
			setLogger(LoggerFactory.getLogger(WriteRecordSetToOutStream.class));
		}

		@Override
		protected Void executeWork() throws CancellationException, Exception {
			m_writer.write(m_stream);
			return null;
		}
	}
}
