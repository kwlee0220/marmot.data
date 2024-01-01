package marmot;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import marmot.support.TypeParser;
import marmot.type.DataType;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStream;
import utils.stream.IntFStream;
import utils.stream.KVFStream;


/**
 * {@code RecordSchema}는 {@link RecordSet}에 포함된 레코드들의 스키마 정보를 표현하는
 * 클래스이다.
 * <p>
 * 하나의 레코드 세트에 포함된 모든 레코드는 동일한 레코드 스키마를 갖는다.
 * 레코드 스키마는 레코드를 구성하는 컬럼({@link Column})들의 리스트로 구성되고,
 * 각 컬럼은 이름과 타입으로 구성된다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSchema implements Serializable  {
	private static final long serialVersionUID = 1L;

	/** Empty RecordSchema */
	public static final RecordSchema NULL = builder().build();
	
	private final Map<String,Column> m_colMap;
	private final Column[] m_columns;
	
	private RecordSchema(RecordSchema.Builder builder) {
		m_colMap = new HashMap<>(builder.m_columns.size());
		m_columns = new Column[builder.m_columns.size()];

		int i = 0;
		for ( Column col: builder.m_columns.values() ) {
			Column copy = new Column(col.name(), col.type(), i);
			
			m_colMap.put(col.name(), copy);
			m_columns[i++] = copy;
		}
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 여부를 반환한다.
	 * 
	 * @param name	대상 컬럼 이름
	 * @return	이름에 해당하는 컬럼이 존재하는 경우는 true, 그렇지 않은 경우는 false
	 */
	public boolean existsColumn(String name) {
		Utilities.checkNotNullArgument(name, "column name is null");
		
		return m_colMap.containsKey(name);
	}
	
	/**
	 * 컬럼이름에 해당하는 컬럼 정보를 반환한다.
	 * 
	 * @param name	컬럼이름
	 * @return	컬럼 정보 객체
	 * @throws ColumnNotFoundException	컬럼이름에 해당하는 컬럼이 존재하지 않는 경우
	 */
	public Column getColumn(String name) {
		return findColumn(name)
				.getOrThrow(() -> new ColumnNotFoundException("name=" + name + ", schema=" + this));
	}
	
	/**
	 * 주어진 이름의 컬럼의 존재 여부를 반환한다.
	 * 만일 해당 이름에 해당하는 컬럼이 존재하지 않는 경우는 {link FOption#empty}를 반환한다.
	 * 
	 * @param name	컬럼이름
	 * @return	컬럼 정보 객체
	 */
	public FOption<Column> findColumn(String name) {
		Utilities.checkNotNullArgument(name, "column name is null");
		
		return FOption.ofNullable(m_colMap.get(name));
	}
	
	/**
	 * 주어진 순번에 해당하는 컬럼 정보를 반환한다.
	 * 
	 * @param idx	컬럼 순번
	 * @return	컬럼 정보 객체
	 */
	public Column getColumnAt(int idx) {
		return m_columns[idx];
	}
	
	/**
	 * 레코드에 정의된 컬럼의 개수를 반환한다.
	 * 
	 * @return	컬럼 개수
	 */
	public int getColumnCount() {
		return m_columns.length;
	}
	
	/**
	 * 레코드에 정의된 모든 컬럼 정보 객체 모음을 반환한다.
	 * 
	 * @return	컬럼 정보 객체 모음
	 * 
	 */
	public List<Column> getColumns() {
		return Arrays.asList(m_columns);
	}

	/**
	 * 레코드 스키마에 정의된 모든 컬럼 객체 스트림을 반환한다.
	 * 
	 * @return	컬럼 스트림
	 */
	public FStream<Column> streamColumns() {
		return FStream.of(m_columns);
	}
	
	/**
	 * 하나 이상의 레코드 스키마를 차례대로 연결하여 하나의 레코드 스키마를 생성한다.
	 * 
	 * @param schemas	연결할 원소 레코드 스키마 배열
	 * @return	연결된 레코드 스키마.
	 */
	public static RecordSchema concat(RecordSchema... schemas) {
		Utilities.checkNotNullArgument(schemas, "RecordSchemas are null");
		
		return FStream.of(schemas)
					.flatMap(s -> FStream.of(s.m_columns))
					.foldLeft(builder(), (b,c) -> b.addColumn(c))
					.build();
	}

	/**
	 * 주어진 키에 포함된 컬럼 이름에 해당하는 레코드 스키마를 생성한다.
	 * 
	 * @param key	검색 컬럼 이름 리스트.
	 * @return	키에 포함된 컬럼 이름으로 구성된 레코드 스키마.
	 */
	public RecordSchema project(Iterable<String> key) {
		Utilities.checkNotNullArgument(key, "name list is null");
		
		return project(FStream.from(key));
	}
	public RecordSchema project(FStream<String> keys) {
		Utilities.checkNotNullArgument(keys, "name list is null");
		
		return keys.flatMapFOption(this::findColumn)
					.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
					.build();
	}
	public RecordSchema project(int... colIdxes) {
		Utilities.checkNotNullArgument(colIdxes, "colIdxes are null");
		
		return IntFStream.of(colIdxes)
						.map(this::getColumnAt)
						.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	/**
	 * 주어진 키에 포함되지 않은 컬럼 이름에 해당하는 레코드 스키마를 생성한다.
	 * 
	 * @param key	여집합 레코드 스키마 생성에 사용할 다중 컬럼 키 객체.
	 * @return	본 키에 포함되지 않은 컬럼으로 구성된 레코드 스키마.
	 * @see #project(List)
	 */
	public RecordSchema complement(Iterable<String> key) {
		Utilities.checkNotNullArgument(key, "key column list is null");
		
		Set<String> names = FStream.from(key).toSet();
		return FStream.of(m_columns)
						.filter(c -> !names.contains(c.name()))
						.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	public RecordSchema complement(String... cols) {
		return complement(Lists.newArrayList(cols));
	}
	public RecordSchema complement(int... colIdxes) {
		Set<Integer> idxes = Sets.newHashSet(Ints.asList(colIdxes));
		return FStream.of(m_columns)
						.zipWithIndex()
						.filter(t -> !idxes.contains(t._2))
						.map(Tuple::_1)
						.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		RecordSchema other = (RecordSchema)obj;
		if ( m_columns.length != other.m_columns.length ) {
			return false;
		}
		
		return Arrays.equals(m_columns, other.m_columns);
	}
	
	@Override
	public String toString() {
		return toDisplayName();
	}
	
	@Override
	public int hashCode() {
		return Utilities.hashCode(FStream.of(m_columns));
	}
	
	public static RecordSchema fromTypeId(String typeId) {
		return TypeParser.parseRecordSchemaFromTypeId(typeId);
	}
	
	public String toTypeId() {
		return streamColumns()
					.map(col -> String.format("%s:%s", col.name(), col.type().id()))
					.join(",", "{", "}");
	}
	
	public String toDisplayName() {
		return streamColumns()
					.map(col -> String.format("%s:%s", col.name(), col.type()))
					.join(",", "record{", "}");
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
		
		private SerializationProxy(RecordSchema schema) {
			m_typeId = schema.toTypeId();
		}
		
		private Object readResolve() {
			return RecordSchema.fromTypeId(m_typeId);
		}
	}
	
	public Builder toBuilder() {
		// 별도 다시 생성하여 사용하지 않으면, column 객체의 공유로 인해
		// 문제가 발생할 수 있음.
		return FStream.of(m_columns).foldLeft(builder(), (b,c) -> b.addColumn(c));
	}

	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private LinkedHashMap<String, Column> m_columns;
		
		private Builder() {
			m_columns = Maps.newLinkedHashMap();
		}
		
		public int size() {
			return m_columns.size();
		}
		
		public RecordSchema build() {
			return new RecordSchema(this);
		}
		
		public boolean existsColumn(String name) {
			Utilities.checkNotNullArgument(name, "column name is null");
			Utilities.checkArgument(name.indexOf(',') == -1,
									() -> "column name should not have ',', name=" + name);
			
			return m_columns.containsKey(name);
		}
		
		public Builder addColumn(String name, DataType type) {
			Utilities.checkNotNullArgument(name, "column name is null");
			Utilities.checkNotNullArgument(type, "column type is null");
			Utilities.checkArgument(name.indexOf(',') == -1,
									() -> "column name should not have ',', name=" + name);
			
			Column col = new Column(name, type, m_columns.size());
			if ( m_columns.putIfAbsent(name, col) != null ) {
				throw new IllegalArgumentException("column already exists: name=" + name);
			}
			return this;
		}
		
		public Builder addColumn(Column col) {
			Utilities.checkNotNullArgument(col, "column is null");
			
			return addColumn(col.name(), col.type());
		}
		
		public Builder addColumnIfAbsent(String name, DataType type) {
			Utilities.checkNotNullArgument(name, "column name is null");
			Utilities.checkNotNullArgument(type, "column type is null");
			Utilities.checkArgument(name.indexOf(',') == -1,
									() -> "column name should not have ',', name=" + name);

			Column col = new Column(name, type, m_columns.size());
			m_columns.putIfAbsent(name, col);
			return this;
		}
		
		public Builder addOrReplaceColumn(String name, DataType type) {
			Utilities.checkNotNullArgument(name, "column name is null");
			Utilities.checkNotNullArgument(type, "column type is null");
			Utilities.checkArgument(name.indexOf(',') == -1,
									() -> "column name should not have ',', but " + name);
			
			Column col = m_columns.get(name);
			if ( col != null ) {
				LinkedHashMap<String, Column> old = m_columns;
				m_columns = new LinkedHashMap<>(old.size());
				FStream.from(old.values())
						.map(c -> c == col ? new Column(name, type) : c)
						.forEach(this::addColumn);
			}
			else {
				addColumn(name, type);
			}
			
			return this;
		}
		
		public Builder addOrReplaceColumn(Column col) {
			Utilities.checkNotNullArgument(col, "column is null");
			Utilities.checkArgument(col.name().indexOf(',') == -1,
									() -> "column name should not have ',', column=" + col.name());
			
			Column prev = m_columns.get(col.name());
			if ( prev != null ) {
				LinkedHashMap<String, Column> old = m_columns;
				m_columns = new LinkedHashMap<>(old.size());
				FStream.from(old.values())
						.map(c -> c == prev ? col : c)
						.forEach(this::addColumn);
			}
			else {
				addColumn(col);
			}
			
			return this;
		}
		
		public Builder addColumnAll(Column... columns) {
			Utilities.checkNotNullArguments(columns, "columns array is null");
			
			for ( Column col: columns ) {
				addColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder addColumnAll(Iterable<Column> columns) {
			Utilities.checkNotNullArgument(columns, "columns list is null");
			
			for ( Column col: columns ) {
				addColumn(col.name(), col.type());
			}
			
			return this;
		}
		
		public Builder addOrReplaceColumnAll(Column... columns) {
			Utilities.checkNotNullArguments(columns, "columns array is null");
			
			for ( Column col: columns ) {
				addOrReplaceColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder addOrReplaceColumnAll(Iterable<Column> columns) {
			Utilities.checkNotNullArgument(columns, "column iterator is null");
			
			for ( Column col: columns ) {
				addOrReplaceColumn(col.name(), col.type());
			}
			return this;
		}
		
		public Builder removeColumn(String colName) {
			Utilities.checkNotNullArgument(colName, "column name is null");
			
			LinkedHashMap<String, Column> old = m_columns;
			m_columns = new LinkedHashMap<>(old.size());
			KVFStream.from(old)
					.filterKey(k -> !k.equals(colName))
					.toValueStream()
					.forEach(this::addColumn);
			
			return this;
		}
		
		@Override
		public String toString() {
			return m_columns.values().
							stream()
							.map(Column::toString)
							.collect(Collectors.joining(","));
		}
	}
}
