package marmot.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.locationtech.jts.geom.Geometry;
import org.postgis.PGgeometry;

import marmot.Column;
import marmot.RecordSchema;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import marmot.type.PrimitiveDataType;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PostgreSQLRecordAdaptor extends JdbcRecordAdaptor {
	public static final String JDBC_DRIVER_CLASS = "org.postgresql.Driver";
	
	public PostgreSQLRecordAdaptor(JdbcProcessor jdbc, RecordSchema schema, GeometryFormat format) {
		super(jdbc, schema, format);
	}

	@Override
	protected String declareBinaryColumn(Column col, boolean notNull) {
		String nonNullStr = notNull ? " not null" : "";
		return String.format("%s bytea%s", col.name(), nonNullStr);
	}

	@Override
	protected String declareNativeGeometryColumn(Column col, boolean notNull) {
		String nonNullStr = notNull ? " not null" : "";
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			switch ( ((PrimitiveDataType)type).typeClass() ) {
				case POLYGON:
					return String.format("%s geometry(Polygon)%s", col.name(), nonNullStr);
				case MULTI_POLYGON:
					return String.format("%s geometry(MultiPolygon)%s", col.name(), nonNullStr);
				case POINT:
					return String.format("%s geometry(Point)%s", col.name(), nonNullStr);
				case MULTI_POINT:
					return String.format("%s geometry(MultiPoint)%s", col.name(), nonNullStr);
				case LINESTRING:
					return String.format("%s geometry(LineString)%s", col.name(), nonNullStr);
				case MULTI_LINESTRING:
					return String.format("%s geometry(MultiLineString)%s", col.name(), nonNullStr);
				case GEOM_COLLECTION:
					return String.format("%s geometry(GeometryCollection)%s", col.name(), nonNullStr);
				case GEOMETRY:
					return String.format("%s geometry%s", col.name(), nonNullStr);
				default:
					throw new AssertionError("invalid GeometryType: " + type);
			}
		}
		else {
			throw new IllegalArgumentException("not Geometry column: column=" + col);
		}
	}
	
	protected Geometry getNativeGeometryColumn(Column col, ResultSet rs, int colIdx)
		throws SQLException {
		PGgeometry geom = (PGgeometry)rs.getObject(colIdx);
		String wkt = geom.getValue();
		return GeometryDataType.fromWkt(wkt);
	}

	@Override
	protected void setNativeGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		pstmt.setObject(idx, new PGgeometry(GeometryDataType.toWkt(geom)));
	}
}
