package marmot;

import com.vividsolutions.jts.geom.Geometry;

import marmot.type.DataType;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LazyGeomRecord implements Record {
	private final RecordSchema m_schema;
	private final Object[] m_values;
	private final Object[] m_cache;
	
	public static LazyGeomRecord of(RecordSchema schema) {
		return new LazyGeomRecord(schema);
	}
	
	private LazyGeomRecord(RecordSchema schema) {
		m_schema = schema;
		m_values = new Object[m_schema.getColumnCount()];
		m_cache = new Object[m_schema.getColumnCount()];
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public Object get(int index) {
		Column col = m_schema.getColumnAt(index);
		if ( col.type().isGeometryType() ) {
			if ( m_cache[index] == null ) {
				m_cache[index] = GeometryDataType.fromWkb((byte[])m_values[index]);
			}
			return m_cache[index];		
		}
		else {
			return m_values[index];
		}
	}

	@Override
	public Record set(int idx, Object value) {
		DataType type = m_schema.getColumnAt(idx).type();
		if ( type.isGeometryType() ) {
			m_cache[idx] = value;
			m_values[idx] = GeometryDataType.toWkb((Geometry)value);
		}
		else {
			m_values[idx] = value;
		}
		return this;
	}
	
	@Override
	public String toString() {
		return m_schema.streamColumns()
						.map(col -> {
							if ( col.type().isGeometryType() ) {
								String repr;
								Object geom = m_cache[col.ordinal()];
								if ( geom != null ) {
									repr = "" + geom;
								}
								else {
									geom = m_values[col.ordinal()];
									if ( geom == null ) {
										geom = "null";
									}
									else {
										geom = "geometry(serialized)";
									}
								}
								return String.format("%s:%s", col.name(), geom);
							}
							else {
								Object v = m_values[col.ordinal()];
								if ( v == null ) {
									v = "null";
								}
								else if ( v instanceof byte[] ) {
									v = String.format("binary[%d]", ((byte[])v).length);
								}
								return String.format("%s:%s", col.name(), v);
							}
						})
						.join(",", "[", "]");
	}
}
