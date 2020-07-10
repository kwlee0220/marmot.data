package marmot.type;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.Maps;

import marmot.RecordSchema;
import marmot.support.TypeParser;
import utils.Throwables;

/**
 * 'Serializable'을 상속하는 이유는 spark 때문임.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class DataType implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final String m_id;
	private final TypeClass m_tc;
	private final Class<?> m_instCls;

	public static final DataType NULL = NullType.get();
	public static final ByteType BYTE = ByteType.get();
	public static final ShortType SHORT = ShortType.get();
	public static final IntType INT = IntType.get();
	public static final LongType LONG = LongType.get();
	public static final FloatType FLOAT = FloatType.get();
	public static final DoubleType DOUBLE = DoubleType.get();
	public static final BooleanType BOOLEAN = BooleanType.get();
	public static final StringType STRING = StringType.get();
	public static final BinaryType BINARY = BinaryType.get();
	
	public static final DateType DATE = DateType.get();
	public static final TimeType TIME = TimeType.get();
	public static final DateTimeType DATETIME = DateTimeType.get();

	public static final CoordinateType COORDINATE = CoordinateType.get();
	public static final EnvelopeType ENVELOPE = EnvelopeType.get();
	
	public static final PointType POINT = PointType.of(null);
	public static final PointType POINT(String srid) {
		return PointType.of(srid);
	}
	public static final MultiPointType MULTI_POINT = MultiPointType.of(null);
	public static final MultiPointType MULTI_POINT(String srid) {
		return MultiPointType.of(srid);
	}
	public static final LineStringType LINESTRING = LineStringType.of(null);
	public static final LineStringType LINESTRING(String srid) {
		return LineStringType.of(srid);
	}
	public static final MultiLineStringType MULTI_LINESTRING = MultiLineStringType.of(null);
	public static final MultiLineStringType MULTI_LINESTRING(String srid) {
		return MultiLineStringType.of(srid);
	}
	public static final PolygonType POLYGON = PolygonType.of(null);
	public static final PolygonType POLYGON(String srid) {
		return PolygonType.of(srid);
	}
	public static final MultiPolygonType MULTI_POLYGON = MultiPolygonType.of(null);
	public static final MultiPolygonType MULTI_POLYGON(String srid) {
		return MultiPolygonType.of(srid);
	}
	public static final GeometryCollectionType GEOM_COLLECTION = GeometryCollectionType.of(null);
	public static final GeometryCollectionType GEOM_COLLECTION(String srid) {
		return GeometryCollectionType.of(srid);
	}
	public static final GeometryType GEOMETRY = GeometryType.of(null);
	public static final GeometryType GEOMETRY(String srid) {
		return GeometryType.of(srid);
	}
	
	public static final ListType LIST = ListType.NULL;
	public static final ListType LIST(DataType elmType) {
		return new ListType(elmType);
	}
	public static final RecordType RECORD = RecordType.NULL;
	public static final RecordType RECORD(RecordSchema schema) {
		return new RecordType(schema);
	}
	
	public abstract String displayName();
	
	/**
	 * 본 데이터 타입의 empty 데이터를 생성한다.
	 * 
	 * @return 데이터 객체.
	 */
	public abstract Object newInstance();
	
	/**
	 * 주어진 스트링 representation을 파싱하여 데이터 객체를 생성한다.
	 * 
	 * @param str	파싱할 대상 데이터 표현 스트림
	 * @return	데이터 객체
	 */
	public abstract Object parseInstance(String str);
	
	/**
	 * 주어진 데이터를 직렬화시킨 값을 출력 스트림에 write한다.
	 * 
	 * @param obj	직렬화시킬 객체.
	 * @param oos	출력 스트림
	 * @throws	IOException	출력 IO 예외
	 */
	public abstract void serialize(Object obj, ObjectOutputStream oos) throws IOException;
	
	/**
	 * 입력 스트림에서 데이터를 읽어 역직렬화한 객체를 반환한다.
	 * 
	 * @param ois	입력 스트림
	 * @return	객체
	 * @throws	IOException	입력 IO 예외
	 */
	public abstract Object deserialize(ObjectInputStream ois) throws IOException;
	
	protected DataType(String id, TypeClass tc, Class<?> instClass) {
		m_id = id;
		m_tc = tc;
		m_instCls = instClass;
	}
	
	public final String id() {
		return m_id;
	}
	
	public final TypeClass typeClass() {
		return m_tc;
	}
	
	public final Class<?> instanceClass() {
		return m_instCls;
	}
	
	public String toInstanceString(Object instance) {
		try {
			return instance.toString();
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
	
	public boolean isPrimitiveType() {
		return this instanceof PrimitiveDataType;
	}
	
	public boolean isGeometryType() {
		return this instanceof GeometryDataType;
	}
	
	public boolean isListType() {
		return this instanceof ListType;
	}
	
	public boolean isRecordType() {
		return this instanceof RecordType;
	}
	
	static final Map<Class<?>,DataType> CLASS_TO_TYPES = Maps.newHashMap();
	static final Map<String, DataType> TC_NAME_TO_TYPES = Maps.newHashMap();
	static DataType[] TYPES = {
		NULL,
		BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, BINARY,
		DATE, TIME, DATETIME,
		COORDINATE, ENVELOPE,
		POINT, MULTI_POINT, LINESTRING, MULTI_LINESTRING, POLYGON, MULTI_POLYGON,
			GEOM_COLLECTION, GEOMETRY,
		LIST, RECORD,
	};
	static {
		Stream.of(TYPES)
				.forEach(type -> { 
					CLASS_TO_TYPES.put(type.instanceClass(), type);
					TC_NAME_TO_TYPES.put(type.typeClass().name(), type);
				});
		CLASS_TO_TYPES.put(java.util.Date.class, DataType.DATETIME);
		CLASS_TO_TYPES.put(java.sql.Date.class, DataType.DATE);
	}
	
	public static DataType fromInstanceClass(Class<?> cls) {
		DataType type = CLASS_TO_TYPES.get(cls);
		if ( type == null ) {
			throw new IllegalArgumentException("unknown DataType instance class: " + cls);
		}
		
		return type;
	}

	/**
	 * 타입 식별자에 해당하는 타입 객체를 반환한다.
	 * 
	 * @param typeId	타입 식별자
	 */
	public static DataType fromTypeId(String typeId) {
		return TypeParser.parseTypeId(typeId);
	}
	
	public static DataType parseDisplayName(String name) {
		return TypeParser.parseTypeName(name);
	}

	/**
	 * 타입 이름에 해당하는 타입 객체를 반환한다.
	 * 
	 * @param typeId	타입 이름
	 */
	public static DataType fromTypeCodeName(String typeName) {
		return TC_NAME_TO_TYPES.get(typeName);
	}

	public static DataType fromTypeCode(int code) {
		return TYPES[code];
	}

	public static DataType fromTypeCode(TypeClass code) {
		return TYPES[code.get()];
	}
	
	@Override
	public String toString() {
		return displayName();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		DataType other = (DataType)obj;
		return m_id.equals(other.m_id);
	}
	
	@Override
	public int hashCode() {
		return m_id.hashCode();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final String m_typeId;
		
		private SerializationProxy(DataType type) {
			m_typeId = type.id();
		}
		
		private Object readResolve() {
			return fromTypeId(m_typeId);
		}
	}
}
