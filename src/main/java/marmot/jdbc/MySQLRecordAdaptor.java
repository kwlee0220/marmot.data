package marmot.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.vividsolutions.jts.geom.Geometry;

import marmot.Column;
import marmot.RecordSchema;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import marmot.type.PrimitiveDataType;
import marmot.type.TypeClass;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MySQLRecordAdaptor extends JdbcRecordAdaptor {
	public MySQLRecordAdaptor(JdbcProcessor jdbc, RecordSchema schema, GeometryFormat format) {
		super(jdbc, schema, format);
	}

	@Override
	protected String declareBinaryColumn(Column col, boolean notNull) {
		String nonNullStr = notNull ? " not null" : "";
		return String.format("%s varbinary%s", col.name(), nonNullStr);
	}

	@Override
	protected String declareSQLColumn(Column col, boolean notNull) {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			switch ( getGeometryFormat() ) {
				case NATIVE:
					return String.format("%s GEOMETRY", col.name());
				case WKB:
					return String.format("%s longblob", col.name());
				case WKT:
					return String.format("%s text", col.name());
				default:
					throw new IllegalArgumentException("unsupported GeometryFormat: " + getGeometryFormat());
			}
		}
		else if ( ((PrimitiveDataType)type).typeClass() == TypeClass.BINARY ) {
			return String.format("%s varbinary", col.name());
		}
		else {
			return super.declareSQLColumn(col, notNull);
		}
	}

	@Override
	protected Geometry getGeometryColumn(Column col, ResultSet rs, int colIdx)
		throws SQLException {
		try {
			switch ( getGeometryFormat() ) {
				case WKB:
					return GeometryDataType.fromWkb(rs.getBinaryStream(colIdx));
				case WKT:
					return GeometryDataType.fromWkt(rs.getString(colIdx));
				default:
					throw new IllegalArgumentException("unsupported GeometryFormat: " + getGeometryFormat());
			}
		}
		catch ( Exception e ) {
			throw new SQLException("" + e);
		}
	}

	@Override
	protected void setGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		switch ( getGeometryFormat() ) {
			case WKB:
				pstmt.setBytes(idx, GeometryDataType.toWkb(geom));
				break;
			case WKT:
			case NATIVE:
				pstmt.setString(idx, GeometryDataType.toWkt(geom));
				break;
			default:
				throw new IllegalArgumentException("unsupported GeometryFormat: " + getGeometryFormat());
		}
	}

	@Override
	public String getInsertValueExpr(Column col) {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			switch ( getGeometryFormat() ) {
				case NATIVE:
					return "GeomFromText(?)";
				default:
					return super.getInsertValueExpr(col);
			}
		}
		else {
			return super.getInsertValueExpr(col);
		}
	}
}
