package marmot;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Geometry;

import marmot.support.DataUtils;
import utils.func.FOption;
import utils.func.KeyValue;
import utils.stream.FStream;
import utils.stream.KVFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Record {
	/**
	 * 레코드의 스키마 객체를 반환한다.
	 * 
	 * @return	스키마 객체.
	 */
	public RecordSchema getRecordSchema();
	
	/**
	 * 컬럼 수를 반환한다.
	 * 
	 * @return	컬럼 수
	 */
	public default int getColumnCount() {
		return getRecordSchema().getColumnCount();
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 유무를 반환한다.
	 * 
	 * @param name	검색 대상 컬럼 이름.
	 * @return	컬럼 존재 여부
	 */
	public default boolean existsColumn(String name) {
		return getRecordSchema().existsColumn(name);
	}

	/**
	 * 주어진 순번에 해당하는 컬럼 값을 반환한다.
	 * 
	 * @param index	대상 컬럼 순번.
	 * @return 컬럼 값.
	 * @throws IndexOutOfBoundsException	컬럼 순번이 유효하지 않은 경우.
	 */
	public Object get(int index);

	/**
	 * 컬럼이름에 해당하는 값을 반환한다.
	 * 
	 * @param name	컬럼이름.
	 * @return 컬럼 값.
	 * @throws ColumnNotFoundException	컬럼이름에 해당하는 컬럼이 존재하지 않는 경우.
	 */
	public default Object get(String name) throws ColumnNotFoundException {
		return get(getRecordSchema().getColumn(name).ordinal());
	}
	
	/**
	 * 레코드의 모든 컬럼 값들을 리스트 형태로 반환한다.
	 * 리스트의 컬럼 값들은 레코드 스키마에 정의된 순서대로 기록된다.
	 * 
	 * @return	컬럼 값 리스트.
	 */
	public default Object[] getAll() {
		Object[] values = new Object[getRecordSchema().getColumnCount()];
		for ( int i =0; i < values.length; ++i ) {
			values[i] = get(i);
		}
		
		return values;
	}
	
	public default Map<String,Object> toMap() {
		return new RecordMap(this);
	}
	
	public default KVFStream<String,Object> fstream() {
		return new KVFStream<String, Object>() {
			private final RecordSchema m_schema = getRecordSchema();
			private int m_idx = 0;

			@Override
			public FOption<KeyValue<String, Object>> next() {
				if ( m_idx >= m_schema.getColumnCount() ) {
					return FOption.empty();
				}
				
				Column col = m_schema.getColumnAt(m_idx);
				Object val = get(m_idx);
				++m_idx;
				
				return FOption.of(KeyValue.of(col.name(), val));
			}

			@Override public void close() throws Exception { }
		};
	}
	
	/**
	 * 이름에 해당하는 컬럼 값을 변경시킨다.
	 * 만일 주어진 이름에 해당하는 컬럼이 없는 경우는 {@link ColumnNotFoundException} 예외를 발생시킨다.
	 * 
	 * @param name	컬럼 이름.
	 * @param value	컬럼 값.
	 * @return	갱신된 레코드 객체.
	 * @throws ColumnNotFoundException	이름에 해당하는 컬럼이 존재하지 않는 경우.
	 */
	public default Record set(String name, Object value) throws ColumnNotFoundException {
		set(getRecordSchema().getColumn(name).ordinal(), value);
		return this;
	}
	
	/**
	 * 순번에 해당하는 컬럼 값을 변경시킨다.
	 * 
	 * @param idx	컬럼 순번.
	 * @param value	컬럼 값.
	 * @return	갱신된 레코드 객체.
	 * @throws ColumnNotFoundException	컬럼 순번이 유효하지 않은 경우.
	 */
	public Record set(int idx, Object value);
	
	/**
	 * 주어진 레코드({@code src})의 모든 컬럼들을 복사해 온다.
	 * src 레코드에 정의된 모든 컬럼 값들 중에서 본 레코드의 동일 이름의 컬럼이 존재하는 경우
	 * 해당 레코드 값으로 복사한다.
	 * 
	 * @param src	값을 복사해 올 대상 레코드.
	 * @return	갱신된 레코드 객체.
	 */
	public default Record set(Record src) {
		src.fstream()
			.mapKey(key -> getRecordSchema().findColumn(key).map(Column::ordinal).getOrElse(-1))
			.filterKey(idx -> idx >= 0)
			.forEach(kv -> set(kv.key(), kv.value()));
		return this;
	}
	
	/**
	 * 맵 객체를 이용하여 레코드 컬럼 값을 설정한다.
	 * 맵 객체의 각 (키, 값) 순서쌍에 대해 키와 동일한 이름의 컬럼 값을 설정한다.
	 * 
	 * @param values 	설정할 값을 가진 맵 객체.
	 * @return	갱신된 레코드 객체.
	 */
	public default Record set(Map<String, Object> values) {
		KVFStream.from(values)
				.mapKey(key -> getRecordSchema().findColumn(key).map(Column::ordinal).getOrElse(-1))
				.filterKey(idx -> idx >= 0)
				.forEach(kv -> set(kv.key(), kv.value()));
		return this;
	}
	
	/**
	 * 주어진 레코드의 모든 컬럼들을 복사해 온다.
	 * 
	 * @param values	설정할 컬럼 값.
	 * 					컬럼 값의 순서는 레코드 스크마에 정의된 컬럼 순서와 같아야 한다.
	 * @return	갱신된 레코드 객체.
	 */
	public default Record setAll(Iterable<?> values) {
		FStream.from(values)
				.take(getRecordSchema().getColumnCount())
				.zipWithIndex().forEach(t -> set(t._2, t._1));
		return this;
	}
	
	/**
	 * 주어진 레코드의 모든 컬럼들을 복사해 온다.
	 * 
	 * @param values	설정할 컬럼 값.
	 * 					컬럼 값의 순서는 레코드 스크마에 정의된 컬럼 순서와 같아야 한다.
	 * @return	갱신된 레코드 객체.
	 */
	public default Record setAll(Object... values) {
		return setAll(Arrays.asList(values));
	}
	
	public default Record setAll(int start, List<?> values) {
		for ( int i = start; i < getColumnCount(); ++i ) {
			set(i, values.get(i-start));
		}
		
		return this;
	}
	
	public default Record setAll(int start, Object[] values) {
		return setAll(start, Arrays.asList(values));
	}
	
	public default void clear() {
		for ( int i =0; i < getRecordSchema().getColumnCount(); ++i ) {
			set(i, null);
		}
	}
	
	/**
	 * 본 레코드를 복사한 레코드를 생성한다.
	 * 
	 * @return	복사된 레코드.
	 */
	public default Record duplicate() {
		Record dup = DefaultRecord.of(getRecordSchema());
		for ( int i =0; i < getRecordSchema().getColumnCount(); ++i ) {
			dup.set(i, get(i));
		}
		return dup;
	}
	
	public default int getInt(int idx) {
		return DataUtils.asInt(get(idx));
	}
	public default int getInt(String name) {
		return DataUtils.asInt(get(name));
	}
	
	public default long getLong(int idx) {
		return DataUtils.asLong(get(idx));
	}
	public default long getLong(String name) {
		return DataUtils.asLong(get(name));
	}
	
	public default short getShort(int idx) {
		return DataUtils.asShort(get(idx));
	}
	public default short getShort(String name) {
		return DataUtils.asShort(get(name));
	}
	
	public default double getDouble(int idx) {
		return DataUtils.asDouble(get(idx));
	}
	public default double getDouble(String name) {
		return DataUtils.asDouble(get(name));
	}
	
	public default float getFloat(int idx) {
		return DataUtils.asFloat(get(idx));
	}
	public default float getFloat(String name) {
		return DataUtils.asFloat(get(name));
	}

	public default boolean getBoolean(int idx) {
		return DataUtils.asBoolean(get(idx));
	}
	public default boolean getBoolean(String name, boolean defValue) {
		return DataUtils.asBoolean(get(name));
	}
	
	public default String getString(int idx) {
		Object v = get(idx);
		return v != null ? v.toString() : null;
	}
	public default String getString(String name) {
		Object v = get(name);
		return v != null ? v.toString() : null;
	}
	
	public default byte getByte(int idx) {
		return DataUtils.asByte(get(idx));
	}
	public default byte getByte(String name) {
		return DataUtils.asByte(get(name));
	}
	
	public default byte[] getBinary(int idx) {
		Object value = get(idx);
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof byte[] ) {
			return (byte[])value;
		}
		else {
			String msg = String.format("invalid column index: index='%d', (not binary, but %s)",
										idx, value.getClass());
			throw new IllegalArgumentException(msg);
		}
	}
	
	public default byte[] getBinary(String name) {
		Object value = get(name);
		if ( value != null ) {
			if ( value instanceof byte[] ) {
				return (byte[])value;
			}
			else {
				String msg = String.format("invalid column type: name='%s', (not binary, but %s)",
											name, value.getClass());
				throw new IllegalArgumentException(msg);
			}
		}
		else {
			return null;
		}
	}
	
	public default Geometry getGeometry(int idx) {
		return (Geometry)get(idx);
	}
	public default Geometry getGeometry(String name) {
		return (Geometry)get(name);
	}
	
	public default LocalDateTime getDateTime(int idx) {
		return (LocalDateTime)get(idx);
	}
	public default LocalDateTime getDateTime(String name) {
		Object v = get(name);
		return v != null ? (LocalDateTime)v : null;
	}
}
