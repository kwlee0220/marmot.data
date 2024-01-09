package marmot.jdbc;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Maps;

import utils.LocalDateTimes;
import utils.LocalDates;
import utils.LocalTimes;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;
import utils.stream.KVFStream;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStreamException;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import marmot.type.PrimitiveDataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class JdbcRecordAdaptor {
	private final JdbcProcessor m_jdbc;
	private final RecordSchema m_schema;
	private final GeometryFormat m_geomFormat;
	
	public static JdbcRecordAdaptor createDefault(JdbcProcessor jdbc, RecordSchema schema, GeometryFormat format) {
		return new DefaultRecordAdaptor(jdbc, schema, format);
	}
	
	public static JdbcRecordAdaptor create(JdbcProcessor jdbc, RecordSchema schema, GeometryFormat format) {
		Class<? extends JdbcRecordAdaptor> procCls = getAdaptorClass(jdbc.getSystem());
		
		try {
			Constructor<? extends JdbcRecordAdaptor> ctor
						= procCls.getConstructor(JdbcProcessor.class, RecordSchema.class, GeometryFormat.class);
			return ctor.newInstance(jdbc, schema, format);
		}
		catch ( Exception e ) {
			throw new IllegalArgumentException("fails to load JdbcRecordAdaptor, system="
												+ jdbc.getSystem() + ", cause=" + e);
		}
	}
	
	protected JdbcRecordAdaptor(JdbcProcessor jdbc, RecordSchema schema, GeometryFormat format) {
		m_jdbc = jdbc;
		m_schema = schema;
		m_geomFormat = format;
	}
	
	public JdbcProcessor getJdbcProcessor() {
		return m_jdbc;
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public GeometryFormat getGeometryFormat() {
		return m_geomFormat;
	}
	
	public static RecordSchema buildRecordSchema(JdbcProcessor jdbc, String tblName) throws SQLException {
		return KVFStream.from(jdbc.getColumns(tblName))
						.mapValue((k,v) -> fromSqlType(v.type(), v.typeName()))
						.map((k,v) -> new Column(k,v))
						.fold(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	public void createTable(String tblName, List<String> primaryKeyCols, List<String> notNullCols) throws SQLException {
		StringBuilder builder = new StringBuilder(String.format("create table %s (", tblName));
		String prmKeyStr = "";
		if ( primaryKeyCols.size() > 0 ) {
			prmKeyStr = FStream.from(primaryKeyCols).join(",", ", primary key (", ")");
		}
		
		String colDefList = m_schema.streamColumns()
									.map(c -> declareSQLColumn(c, notNullCols.contains(c.name())))
									.join(", ");
		String sqlStr = builder.append(colDefList)
								.append(prmKeyStr)
								.append(")")
								.toString();
		m_jdbc.executeQuery(sqlStr);
	}
	
	public void deleteTable(String tblName) throws SQLException {
		String sqlStr = String.format("drop table %s", tblName);
		try ( Connection conn = m_jdbc.connect();
			Statement stmt = conn.createStatement() ) {
			stmt.executeUpdate(sqlStr);
		}
	}
	
	public void loadRecord(ResultSet rs, Record record) throws SQLException {
		for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
			Column col = m_schema.getColumnAt(i);
			record.set(i, getColumn(col, rs, i+1));
		}
	}

	public void storeRecord(Record record, PreparedStatement pstmt) throws SQLException {
		RecordSchema schema = record.getRecordSchema();
		Object[] values = record.getAll();

		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			Column col = schema.getColumnAt(i);
			setColumn(pstmt, i+1, col, values[i]);
		}
	}
	
	public static DataType fromSqlType(int type, String typeName) {
		switch ( type ) {
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				return DataType.STRING;
			case Types.INTEGER:
				return DataType.INT;
			case Types.DOUBLE:
			case Types.NUMERIC:
				return DataType.DOUBLE;
			case Types.FLOAT:
			case Types.REAL:
				return DataType.FLOAT;
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				return DataType.BINARY;
			case Types.BIGINT:
				return DataType.LONG;
			case Types.BOOLEAN:
				return DataType.BOOLEAN;
			case Types.SMALLINT:
				return DataType.SHORT;
			case Types.TINYINT:
				return DataType.BYTE;
			case Types.TIMESTAMP:
				return DataType.DATETIME;
			case Types.OTHER:
				if ( typeName.equals("geometry") ) {
					return DataType.GEOMETRY;
				}
				else {
					return DataType.NULL;
				}
			default:
				throw new IllegalArgumentException("unsupported SqlTypes: type=" + typeName
													+ ", code=" + type);
		}
	}
	
	protected String declareSQLColumn(Column col, boolean notNull) {
		if ( col.type().isGeometryType() ) {
			return declareGeometryColumn(col, notNull);
		}
		else if ( !col.type().isPrimitiveType() ) {
			throw new AssertionError();
		}
		String nonNullStr = notNull ? " not null" : "";
		switch ( ((PrimitiveDataType)col.type()).typeClass() ) {
			case STRING:
			case ENVELOPE:
				return String.format("%s text%s", col.name(), nonNullStr);
			case INT:
				return String.format("%s int%s", col.name(), nonNullStr);
			case LONG:
				return String.format("%s bigint%s", col.name(), nonNullStr);
			case DOUBLE:
				return String.format("%s double precision%s", col.name(), nonNullStr);
			case DATETIME:
				return String.format("%s timestamp%s", col.name(), nonNullStr);
			case DATE:
				return String.format("%s date%s", col.name(), nonNullStr);
			case TIME:
				return String.format("%s time%s", col.name(), nonNullStr);
			case BOOLEAN:
				return String.format("%s boolean%s", col.name(), nonNullStr);
			case BYTE:
				return String.format("%s tinyint%s", col.name(), nonNullStr);
			case FLOAT:
				return String.format("%s float%s", col.name(), nonNullStr);
			case SHORT:
				return String.format("%s smallint%s", col.name(), nonNullStr);
			case BINARY:
				return declareBinaryColumn(col, notNull);
			default:
				throw new RecordStreamException("unsupported DataType: " + col);
		}
	}
	
	protected String declareBinaryColumn(Column col, boolean notNull) {
		String nonNullStr = notNull ? " not null" : "";
		return String.format("%s bytea%s", col.name(), nonNullStr);
	}
	
	private String declareGeometryColumn(Column col, boolean notNull) {
		switch ( m_geomFormat ) {
			case NATIVE:
				return declareNativeGeometryColumn(col, notNull);
			case WKB:
				return declareWKBGeomColumn(col, notNull);
			case WKT:
				throw new UnsupportedOperationException();
			default:
				throw new AssertionError();
		}
	}
	
	protected String declareNativeGeometryColumn(Column col, boolean notNull) {
		return declareWKBGeomColumn(col, notNull);
	}
	
	protected String declareWKBGeomColumn(Column col, boolean notNull) {
		String nonNullStr = notNull ? " not null" : "";
		switch ( ((PrimitiveDataType)col.type()).typeClass() ) {
			case POLYGON:
				return String.format("%s bytea%s", col.name(), nonNullStr);
			case MULTI_POLYGON:
				return String.format("%s bytea%s", col.name(), nonNullStr);
			case POINT:
				return String.format("%s bytea%s", col.name(), nonNullStr);
			case MULTI_POINT:
				return String.format("%s bytea%s", col.name(), nonNullStr);
			case LINESTRING:
				return String.format("%s bytea%s", col.name(), nonNullStr);
			case MULTI_LINESTRING:
				return String.format("%s bytea%s", col.name(), nonNullStr);
			case GEOM_COLLECTION:
				return String.format("%s bytea%s", col.name(), nonNullStr);
			case GEOMETRY:
				return String.format("%s bytea%s", col.name(), nonNullStr);
			default:
				throw new AssertionError("invalid GeometryType column: " + col);
		}
	}
	
	protected Geometry getGeometryColumn(Column col, ResultSet rs, int colIdx) throws SQLException {
		switch ( m_geomFormat ) {
			case WKB:
				return getWkbGeometryColumn(col, rs, colIdx);
			case NATIVE:
				return getNativeGeometryColumn(col, rs, colIdx);
			case WKT:
				return getWktGeometryColumn(col, rs, colIdx);
			default:
				throw new AssertionError("unsupported GeometryFormat: " + m_geomFormat);
		}
	}
	
	protected Geometry getNativeGeometryColumn(Column col, ResultSet rs, int colIdx) throws SQLException {
		return getWkbGeometryColumn(col, rs, colIdx);
	}
	
	protected Geometry getWkbGeometryColumn(Column col, ResultSet rs, int colIdx) throws SQLException {
		try ( InputStream is = rs.getBinaryStream(colIdx) ) {
			return GeometryDataType.fromWkb(is);
		}
		catch ( Exception e ) {
			throw new SQLException("invalid WKB, cause=" + e);
		}
	}
	
	protected Geometry getWktGeometryColumn(Column col, ResultSet rs, int colIdx) throws SQLException {
		return GeometryDataType.fromWkt(rs.getString(colIdx));
	}
	
	protected void setGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		switch ( m_geomFormat ) {
			case WKB:
				setWkbGeometryColumn(pstmt, idx, col, geom);
				break;
			case WKT:
				setWktGeometryColumn(pstmt, idx, col, geom);
				break;
			case NATIVE:
				setNativeGeometryColumn(pstmt, idx, col, geom);
				break;
			default:
				throw new IllegalStateException("unsupported GeometryFormat: " + m_geomFormat);
		}
	}
	
	protected void setWkbGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		byte[] wkb = GeometryDataType.toWkb(geom);
		pstmt.setBytes(idx, wkb);
	}
	
	protected void setWktGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		String wkt = GeometryDataType.toWkt(geom);
		pstmt.setString(idx, wkt);
	}
	
	protected void setNativeGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		setWkbGeometryColumn(pstmt, idx, col, geom);
	}
	
	public String getInsertValueExpr(Column col) {
		return "?";
	}
	
	private Object getColumn(Column col, ResultSet rs, int colIdx) throws SQLException {
		if ( col.type().isGeometryType() ) {
			return getGeometryColumn(col, rs, colIdx);
		}
		else if ( col.type().isPrimitiveType() ) {
			switch ( ((PrimitiveDataType)col.type()).typeClass() ) {
				case STRING:
					return rs.getString(colIdx);
				case INT:
					return rs.getInt(colIdx);
				case LONG:
					return rs.getLong(colIdx);
				case DOUBLE:
					return rs.getDouble(colIdx);
				case BINARY:
					return rs.getBytes(colIdx);
				case FLOAT:
					return rs.getFloat(colIdx);
				case DATETIME:
					return rs.getTimestamp(colIdx).toLocalDateTime();
				case BOOLEAN:
					return rs.getBoolean(colIdx);
				case DATE:
					return rs.getDate(colIdx).toLocalDate();
				case TIME:
					return rs.getTime(colIdx).toLocalTime();
				default:
					throw new RecordStreamException("unexpected DataType: " + col.type());
			}
		}
		else {
			throw new UnsupportedOperationException();
		}
	}
	
	private void setColumn(PreparedStatement pstmt, int idx, Column col, Object value) throws SQLException {
		if ( col.type().isGeometryType() ) {
			setGeometryColumn(pstmt, idx, col, (Geometry)value);
		}
		else if ( col.type().isPrimitiveType() ) {
			switch ( ((PrimitiveDataType)col.type()).typeClass() ) {
				case STRING:
					// PostgreSQL의 경우 문자열에 '0x00'가 포함되는 경우
					// 오류를 발생시키기 때문에, 삽입전에 제거시킨다.
					String str = (String)value;
					if ( str != null ) {
						str = str.replaceAll("\\x00","");
					}
					pstmt.setString(idx, str);
					break;
				case INT:
					pstmt.setInt(idx, (Integer)value);
					break;
				case LONG:
					pstmt.setLong(idx, (Long)value);
					break;
				case SHORT:
					pstmt.setShort(idx, (Short)value);
					break;
				case DOUBLE:
					pstmt.setDouble(idx, (Double)value);
					break;
				case FLOAT:
					pstmt.setFloat(idx, (Float)value);
					break;
				case BINARY:
					pstmt.setBytes(idx, (byte[])value);
					break;
				case DATETIME:
					pstmt.setLong(idx, LocalDateTimes.toUtcMillis((LocalDateTime)value));
					break;
				case DATE:
					pstmt.setLong(idx, LocalDates.toUtcMillis((LocalDate)value));
					break;
				case TIME:
					pstmt.setString(idx, LocalTimes.toString((LocalTime)value));
					break;
				case BOOLEAN:
					pstmt.setBoolean(idx, (Boolean)value);
					break;
				default:
					throw new RecordStreamException("unexpected DataType: " + col.type());
			}
		}
		else {
			throw new UnsupportedOperationException();
		}
	}
	
	private static Class<? extends JdbcRecordAdaptor> getAdaptorClass(String protocol) {
		return KVFStream.from(JDBC_PROCESSORS)
						.filter(kv -> kv.key().equals(protocol))
						.next()
						.map(kv -> kv.value())
						.getOrThrow(() -> new IllegalArgumentException("unsupported Jdbc protocol: " + protocol));

	}
	
	private static final Map<String,Class<? extends JdbcRecordAdaptor>> JDBC_PROCESSORS;
	static {
		JDBC_PROCESSORS = Maps.newHashMap();
		JDBC_PROCESSORS.put("postgresql", PostgreSQLRecordAdaptor.class);
		JDBC_PROCESSORS.put("mysql", MySQLRecordAdaptor.class);
	}
}
