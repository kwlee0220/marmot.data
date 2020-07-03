package marmot.support;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import marmot.type.DataType;
import marmot.type.PrimitiveDataType;
import utils.LocalDates;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataUtils {
	private DataUtils() {
		throw new AssertionError("Should not be called: " + DataUtils.class);
	}
	
	public static Object cast(Object obj, DataType type) {
		if ( obj == null ) {
			return null;
		}
		else if ( type.instanceClass().isInstance(obj) ) {
			return obj;
		}
		if ( !(type instanceof PrimitiveDataType) ) {
			throw new UnsupportedOperationException("cannot cast to non-primitive type: " + type);
		}
		
		switch ( ((PrimitiveDataType)type).typeClass() ) {
			case STRING:
				return asString(obj);
			case INT:
				return asInt(obj);
			case LONG:
				return asLong(obj);
			case SHORT:
				return asShort(obj);
			case DOUBLE:
				return asDouble(obj);
			case FLOAT:
				return asFloat(obj);
			case BOOLEAN:
				return asBoolean(obj);
			case BYTE:
				return asByte(obj);
			case DATETIME:
				return asDatetime(obj);
			case DATE:
				return asDate(obj);
			case TIME:
				return asTime(obj);
			case BINARY:
				if ( obj instanceof byte[] ) {
					return obj;
				}
				else {
					throw new UnsupportedOperationException("cannot cast non-binary to binary: src" + obj);
				}
			case COORDINATE:
				if ( obj instanceof Coordinate ) {
					return obj;
				}
				else {
					throw new UnsupportedOperationException("cannot cast non-Coordinate to Coordinate: src" + obj);
				}
			case ENVELOPE:
				if ( obj instanceof Envelope ) {
					return obj;
				}
				else {
					throw new UnsupportedOperationException("cannot cast non-Envelope to Envelope: src" + obj);
				}
			case POINT:
				return (Point)obj;
			case POLYGON:
				return (Polygon)obj;
			case MULTI_POLYGON:
				return (MultiPolygon)obj;
			case LINESTRING:
				return (LineString)obj;
			case MULTI_LINESTRING:
				return (MultiLineString)obj;
			case MULTI_POINT:
				return (MultiPoint)obj;
			case GEOM_COLLECTION:
				return (GeometryCollection)obj;
			case GEOMETRY:
				return (Geometry)obj;
			default:
				throw new UnsupportedOperationException("cast: target type=" + type);
		}
	}
	
	public static String asString(Object obj) {
		return obj != null ? obj.toString() : null;
	}
	
	public static int asInt(Object obj) {
		if ( obj == null ) {
			return 0;
		}
		if ( obj instanceof Integer ) {
			return ((Integer)obj).intValue();
		}
		if ( obj instanceof Long ) {
			return (int)((Long)obj).intValue();
		}
		if ( obj instanceof String ) {
			return Integer.parseInt(((String)obj).trim());
		}
		if ( obj instanceof Double ) {
			return ((Double)obj).intValue();
		}
		if ( obj instanceof Boolean ) {
			return ((Boolean)obj) ? 1 : 0;
		}
		
		throw new IllegalArgumentException("unknown int value=" + obj);
	}
	
	public static long asLong(Object obj) {
		if ( obj == null ) {
			return 0;
		}
		if ( obj instanceof Integer ) {
			return ((Integer)obj).intValue();
		}
		if ( obj instanceof Long ) {
			return ((Long)obj).longValue();
		}
		if ( obj instanceof Short ) {
			return ((Short)obj).shortValue();
		}
		if ( obj instanceof Byte ) {
			return ((Byte)obj).byteValue();
		}
		if ( obj instanceof String ) {
			return Long.parseLong(((String)obj).trim());
		}
		if ( obj instanceof Double ) {
			return ((Double)obj).longValue();
		}
		if ( obj instanceof Boolean ) {
			return ((Boolean)obj) ? 1 : 0;
		}
		
		throw new IllegalArgumentException("unknown long value=" + obj);
	}
	
	public static short asShort(Object obj) {
		if ( obj == null ) {
			return 0;
		}
		if ( obj instanceof Short ) {
			return ((Short)obj).shortValue();
		}
		if ( obj instanceof Integer ) {
			return ((Integer)obj).shortValue();
		}
		if ( obj instanceof Long ) {
			return (short)((Long)obj).shortValue();
		}
		if ( obj instanceof String ) {
			return Short.parseShort(((String)obj).trim());
		}
		if ( obj instanceof Double ) {
			return ((Double)obj).shortValue();
		}
		if ( obj instanceof Boolean ) {
			return (short)(((Boolean)obj) ? 1 : 0);
		}
		
		throw new IllegalArgumentException("unknown short value=" + obj);
	}
	
	public static byte asByte(Object obj) {
		if ( obj == null ) {
			return 0;
		}
		if ( obj instanceof Byte ) {
			return ((Byte)obj).byteValue();
		}
		if ( obj instanceof Integer ) {
			return (byte)((Integer)obj).intValue();
		}
		if ( obj instanceof Long ) {
			return (byte)((Long)obj).longValue();
		}
		if ( obj instanceof String ) {
			return Byte.parseByte(((String)obj).trim());
		}
		if ( obj instanceof Double ) {
			return (byte)((Double)obj).longValue();
		}
		if ( obj instanceof Boolean ) {
			return (byte)(((Boolean)obj) ? 1 : 0);
		}
		
		throw new IllegalArgumentException("unknown byte value=" + obj);
	}
	
	public static boolean asBoolean(Object obj, boolean defValue) {
		if ( obj == null ) {
			return defValue;
		}
		if ( obj instanceof Boolean ) {
			return ((Boolean)obj).booleanValue();
		}
		if ( obj instanceof Integer ) {
			return ((Integer)obj).intValue() != 0;
		}
		if ( obj instanceof Long ) {
			return ((Long)obj).longValue() != 0;
		}
		if ( obj instanceof String ) {
			String str = ((String)obj).trim();
			if ( str.equalsIgnoreCase("false") || str.equalsIgnoreCase("no") ) {
				return false;
			}
			return str.length() > 0;
		}
		if ( obj instanceof Double ) {
			return ((Double)obj).compareTo(0.0) != 0;
		}
		
		throw new IllegalArgumentException("unknown boolean value=" + obj);
	}
	
	public static float asFloat(Object obj) {
		if ( obj == null ) {
			return 0;
		}
		if ( obj instanceof Float ) {
			return ((Float)obj).floatValue();
		}
		if ( obj instanceof Integer ) {
			return ((Integer)obj).floatValue();
		}
		if ( obj instanceof Long ) {
			return ((Long)obj).floatValue();
		}
		if ( obj instanceof String ) {
			return Float.parseFloat(((String)obj).trim());
		}
		if ( obj instanceof Double ) {
			return ((Double)obj).floatValue();
		}
		if ( obj instanceof Boolean ) {
			return ((Boolean)obj) ? 1 : 0;
		}
		
		throw new IllegalArgumentException("unknown float value='" + obj + "'");
	}
	
	public static double asDouble(Object obj) {
		if ( obj == null ) {
			return 0;
		}
		if ( obj instanceof Double ) {
			return ((Double)obj).doubleValue();
		}
		if ( obj instanceof Float ) {
			return ((Float)obj).doubleValue();
		}
		if ( obj instanceof Integer ) {
			return ((Integer)obj).doubleValue();
		}
		if ( obj instanceof Long ) {
			return ((Long)obj).doubleValue();
		}
		if ( obj instanceof String ) {
			return Double.parseDouble(((String)obj).trim());
		}
		if ( obj instanceof Boolean ) {
			return ((Boolean)obj) ? 1 : 0;
		}
		
		throw new IllegalArgumentException("unknown double value='" + obj + "'");
	}
	
	public static boolean asBoolean(Object obj) {
		return asBoolean(obj, false);
	}
	
	public static Geometry asGeometry(Object obj) {
		if ( obj instanceof Geometry ) {
			return (Geometry)obj;
		}
		else {
			throw new IllegalArgumentException("unsupport type conversion to Geometry: from=" + obj);
		}
	}
	
	public static LocalDateTime asDatetime(Object obj) {
		if ( obj instanceof LocalDateTime ) {
			return (LocalDateTime)obj;
		}
		else if ( obj instanceof Date ) {
			return ((Date)obj).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		}
		else {
			throw new IllegalArgumentException("Not DateTime object: obj=" + obj);
		}
	}
	
	public static LocalDate asDate(Object obj) {
		if ( obj instanceof java.sql.Date ) {
			return ((java.sql.Date)obj).toLocalDate();
		}
		else if ( obj instanceof LocalDate ) {
			return (LocalDate)obj;
		}
		else if ( obj instanceof Date ) {
			long utcMillis = ((Date)obj).getTime();
			return LocalDates.fromUtcMillis(utcMillis);
		}
		else {
			throw new IllegalArgumentException("Not Date object: obj=" + obj);
		}
	}
	
	public static LocalTime asTime(Object obj) {
		if ( obj instanceof LocalTime ) {
			return (LocalTime)obj;
		}
		else {
			throw new IllegalArgumentException("Not asTime: obj=" + obj);
		}
	}
}
