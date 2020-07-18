package marmot.optor.geo;

import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;

import marmot.Column;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.geo.GeomOpOptions;
import marmot.stream.geo.SpatialRecordLevelTransform;
import marmot.type.GeometryDataType;
import utils.Utilities;
import utils.geo.util.CRSUtils;
import utils.geo.util.CoordinateTransform;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TransformSridReader implements RecordReader {
	private final RecordReader m_input;
	private final Column m_geomCol;
	private final String m_tarSrid;
	private final RecordSchema m_schema;
	private final CoordinateTransform m_trans;
		
	public TransformSridReader(RecordReader input, String srcGeomColName, String tarSrid) {
		Utilities.checkNotNullArgument(input, "input RecordReader");
		Utilities.checkNotNullArgument(srcGeomColName, "source geometry column");
		Utilities.checkNotNullArgument(tarSrid, "target srid");
		
		m_input = input;
		m_tarSrid = tarSrid;
		m_geomCol = input.getRecordSchema().findColumn(srcGeomColName)
									.getOrThrow(() -> new IllegalArgumentException("source geometry column: name=" + srcGeomColName));
		if ( !m_geomCol.type().isGeometryType() ) {
			throw new IllegalArgumentException("source column is not geometry one: name=" + srcGeomColName);
		}
		GeometryDataType srcType = (GeometryDataType)m_geomCol.type();
		
		String srcSrid = srcType.srid();
		m_trans = getCoordinateTransform(srcSrid, tarSrid);
		
		m_schema = input.getRecordSchema().toBuilder()
						.addOrReplaceColumn(srcGeomColName, srcType.duplicate(tarSrid))
						.build();
	}
	
	public static CoordinateTransform getCoordinateTransform(String srcSrid, String tarSrid) {
		CoordinateReferenceSystem srcCrs = CRSUtils.toCRS(srcSrid);
		CoordinateReferenceSystem tarCrs = CRSUtils.toCRS(tarSrid);
		return CoordinateTransform.get(srcCrs, tarCrs);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public String getSourceSrid() {
		return ((GeometryDataType)m_geomCol.type()).srid();
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_input.read());
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s(%s->%s)", getClass().getSimpleName(),
								m_geomCol.name(), getSourceSrid(), m_tarSrid);
	}

	private class StreamImpl extends SpatialRecordLevelTransform {
		public StreamImpl(RecordStream input) {
			super(input, m_geomCol.name(), GeomOpOptions.DEFAULT);
		}

		@Override
		protected GeometryDataType initialize(GeometryDataType inGeomType) {
			return null;
		}
		
		@Override
		protected Geometry transform(Geometry src) {
			try {
				return (m_trans != null) ? m_trans.transform(src) : src;
			}
			catch ( Exception e ) {
				return m_inputGeomType.newInstance();
			}
		}
		
		@Override
		public String toString() {
			return String.format("%s: %s(%s)->%s(%s)", getClass().getSimpleName(),
								m_geomCol.name(), getSourceSrid(), m_tarSrid);
		}
	}
}
