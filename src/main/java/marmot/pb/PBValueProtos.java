package marmot.pb;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.protobuf.ByteString;

import utils.LocalDateTimes;
import utils.LocalDates;
import utils.LocalTimes;
import utils.Tuple;

import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.RecordProto;
import marmot.proto.TypeCodeProto;
import marmot.proto.ValueProto;
import marmot.type.GeometryDataType;
import marmot.type.TypeClass;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBValueProtos {
	public static ValueProto NONE = ValueProto.newBuilder().build();
	
	private PBValueProtos() {
		throw new AssertionError("Should not be called: " + getClass());
	}
	
	public static void fromRecordProto(RecordProto proto, Record output) {
		RecordSchema schema = output.getRecordSchema();
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			PBValueProtos.fromProto(proto.getColumn(i));
			output.set(i, PBValueProtos.fromProto(proto.getColumn(i)));
		}
	}
	
	public static RecordProto toRecordProto(Record record) {
		return record.getRecordSchema()
					.streamColumns()
					.map(col -> toValueProto(col.type().typeClass(), record.get(col.ordinal())))
					.fold(RecordProto.newBuilder(), (b,v) -> b.addColumn(v))
					.build();
	}
	
	//
	// TypeCode 정보가 있는 경우의 Object transform
	//
	public static ValueProto toValueProto(TypeClass tc, Object obj) {
		ValueProto.Builder builder = ValueProto.newBuilder();
		
		if ( obj == null ) {
			return builder.setNullValue(TypeCodeProto.valueOf(tc.name())).build();
		}
		switch ( tc ) {
			case BYTE:
				builder.setByteValue((byte)obj);
				break;
			case SHORT:
				builder.setShortValue((short)obj);
				break;
			case INT:
				builder.setIntValue((int)obj);
				break;
			case LONG:
				builder.setLongValue((long)obj);
				break;
			case FLOAT:
				builder.setFloatValue((float)obj);
				break;
			case DOUBLE:
				builder.setDoubleValue((double)obj);
				break;
			case BOOLEAN:
				builder.setBoolValue((boolean)obj);
				break;
			case STRING:
				builder.setStringValue((String)obj);
				break;
			case BINARY:
				builder.setBinaryValue(ByteString.copyFrom((byte[])obj));
				break;
			case DATETIME:
				builder.setDatetimeValue(LocalDateTimes.toUtcMillis((LocalDateTime)obj));
				break;
			case DATE:
				builder.setDateValue(LocalDates.toEpochMillis((LocalDate)obj));
				break;
			case TIME:
				builder.setTimeValue(LocalTimes.toString((LocalTime)obj));
				break;
			case COORDINATE:
				builder.setCoordinateValue(PBDataSetUtils.toProto((Coordinate)obj));
				break;
			case ENVELOPE:
				builder.setEnvelopeValue(PBDataSetUtils.toProto((Envelope)obj));
				break;
			case POINT:
			case MULTI_POINT:
			case LINESTRING:
			case MULTI_LINESTRING:
			case POLYGON:
			case MULTI_POLYGON:
			case GEOM_COLLECTION:
			case GEOMETRY:
				builder.setGeometryValue(PBDataSetUtils.toProto((Geometry)obj));
				break;
			default:
				throw new AssertionError("tc=" + tc);
		}
		
		return builder.build();
	}

	//
	// TypeCode 정보가 없는 경우의 Object transform
	//
	public static ValueProto toValueProto(Object obj) {
		if ( obj == null ) {
			return ValueProto.newBuilder().build();
		}
		
		ValueProto.Builder builder = ValueProto.newBuilder();
		if ( obj instanceof String ) {
			builder.setStringValue((String)obj);
		}
		else if ( obj instanceof Integer ) {
			builder.setIntValue((int)obj);
		}
		else if ( obj instanceof Double ) {
			builder.setDoubleValue((double)obj);
		}
		else if ( obj instanceof Long ) {
			builder.setLongValue((long)obj);
		}
		else if ( obj instanceof Boolean ) {
			builder.setBoolValue((boolean)obj);
		}
		else if ( obj instanceof Coordinate ) {
			builder.setCoordinateValue(PBDataSetUtils.toProto((Coordinate)obj));
		}
		else if ( obj instanceof Geometry ) {
			builder.setGeometryValue(PBDataSetUtils.toProto((Geometry)obj));
		}
		else if ( obj instanceof Envelope ) {
			builder.setEnvelopeValue(PBDataSetUtils.toProto((Envelope)obj));
		}
		else if ( obj instanceof Byte[] ) {
			builder.setBinaryValue(ByteString.copyFrom((byte[])obj));
		}
		else if ( obj instanceof Byte ) {
			builder.setByteValue((byte)obj);
		}
		else if ( obj instanceof Short ) {
			builder.setShortValue((short)obj);
		}
		else if ( obj instanceof Float ) {
			builder.setFloatValue((float)obj);
		}
		else if ( obj instanceof LocalDateTime ) {
			builder.setDatetimeValue(LocalDateTimes.toEpochMillis((LocalDateTime)obj));
		}
		else if ( obj instanceof LocalDate ) {
			builder.setDateValue(LocalDates.toEpochMillis((LocalDate)obj));
		}
		else if ( obj instanceof LocalTime ) {
			builder.setTimeValue(((LocalTime)obj).toString());
		}
		else {
			throw new AssertionError("unknown object: " + obj);
		}
		
		return builder.build();
	}
	
	//
	//
	//

	public static Tuple<TypeClass,Object> fromProto(ValueProto proto) {
		switch ( proto.getValueCase() ) {
			case BYTE_VALUE:
				return Tuple.of(TypeClass.BYTE, (byte)proto.getByteValue());
			case SHORT_VALUE:
				return Tuple.of(TypeClass.SHORT, (short)proto.getShortValue());
			case INT_VALUE:
				return Tuple.of(TypeClass.INT, (int)proto.getIntValue());
			case LONG_VALUE:
				return Tuple.of(TypeClass.LONG, proto.getLongValue());
			case FLOAT_VALUE:
				return Tuple.of(TypeClass.FLOAT, proto.getFloatValue());
			case DOUBLE_VALUE:
				return Tuple.of(TypeClass.DOUBLE, proto.getDoubleValue());
			case BOOL_VALUE:
				return Tuple.of(TypeClass.BOOLEAN, proto.getBoolValue());
			case STRING_VALUE:
				return Tuple.of(TypeClass.STRING, proto.getStringValue());
			case BINARY_VALUE:
				return Tuple.of(TypeClass.BINARY, proto.getBinaryValue().toByteArray());
				
			case DATETIME_VALUE:
				return Tuple.of(TypeClass.DATETIME, LocalDateTimes.fromUtcMillis(proto.getDatetimeValue()));
			case DATE_VALUE:
				return Tuple.of(TypeClass.DATE, LocalDates.fromEpochMillis(proto.getDateValue()));
			case TIME_VALUE:
				return Tuple.of(TypeClass.TIME, LocalTimes.fromString(proto.getTimeValue()));
			
			case COORDINATE_VALUE:
				return Tuple.of(TypeClass.COORDINATE, proto.getCoordinateValue());
			case ENVELOPE_VALUE:
				return Tuple.of(TypeClass.ENVELOPE, PBDataSetUtils.fromProto(proto.getEnvelopeValue()));
			case GEOMETRY_VALUE:
				Geometry geom = PBDataSetUtils.fromProto(proto.getGeometryValue());
				return Tuple.of(GeometryDataType.fromGeometry(geom), geom);
			case NULL_VALUE:
				TypeClass tc = TypeClass.valueOf(proto.getNullValue().name());
				return Tuple.of(tc, null);
			case VALUE_NOT_SET:
				return Tuple.of(null, null);
			default:
				throw new AssertionError("unknown ValueCase: " + proto.getValueCase());
		}
	}
}
