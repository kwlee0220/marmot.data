package marmot.stream.geo;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.RecordLevelTransformedStream;
import marmot.type.GeometryDataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialRecordLevelTransform extends RecordLevelTransformedStream {
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialRecordLevelTransform.class);
	
	private final String m_inputGeomCol;
	protected final GeomOpOptions m_options;
	private final RecordSchema m_schema;

	private final int m_inputGeomColIdx;
	protected GeometryDataType m_inputGeomType;
	private final Column m_outputGeomCol;
	private final boolean m_throwOpError;

	abstract protected GeometryDataType initialize(GeometryDataType inGeomType);
	protected GeometryDataType initialize(GeometryDataType inGeomType, RecordSchema inputSchema) {
		return initialize(inGeomType);
	}
	abstract protected Geometry transform(Geometry geom);
	protected Geometry transform(Geometry geom, Record inputRecord) {
		return transform(geom);
	}
	
	protected SpatialRecordLevelTransform(RecordStream input, String inGeomCol,
											GeomOpOptions opts) {
		super(input);
		
		Utilities.checkNotNullArgument(inGeomCol, "input geometry column is null");
		Utilities.checkNotNullArgument(opts, "GeomOpOptions is null");
		
		m_inputGeomCol = inGeomCol;
		m_options = opts;

		RecordSchema inputSchema = getInputRecordSchema();
		m_inputGeomColIdx = inputSchema.getColumn(m_inputGeomCol).ordinal();

		Column col = inputSchema.getColumnAt(m_inputGeomColIdx);
		GeometryDataType outGeomType = initialize((GeometryDataType)col.type(), inputSchema);
		
		String outputGeomCol = m_options.outputColumn().getOrElse(m_inputGeomCol);
		m_schema = inputSchema.toBuilder()
								.addOrReplaceColumn(outputGeomCol, outGeomType)
								.build();
		m_outputGeomCol = m_schema.getColumn(outputGeomCol);
		m_throwOpError = m_options.throwOpError().getOrElse(false);
		
		setLogger(s_logger);
	}

	@Override
	protected void closeInGuard() throws Exception {
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public String getInputGeometryColumn() {
		return m_inputGeomCol;
	}
	
	public int getInputGeometryColumnIndex() {
		return m_inputGeomColIdx;
	}
	
	public Column getOutputGeometryColumn() {
		return m_outputGeomCol;
	}

	@Override
	protected boolean transform(Record input, Record output) {
		try {
			output.set(input);
			
			Geometry geom = input.getGeometry(m_inputGeomColIdx);
			if ( geom == null || geom.isEmpty() ) {
				output.set(m_outputGeomCol.ordinal(), handleNullEmptyGeometry(geom));
			}
			else {
				Geometry transformed = transform(geom, input);
				output.set(m_outputGeomCol.ordinal(), transformed);
			}
			
			return true;
		}
		catch ( Exception e ) {
			if ( getLogger().isDebugEnabled() ) {
				getLogger().warn("fails to transform geometry: cause=" + e);
			}
			
			if ( m_throwOpError ) {
				throw e;
			}
			
			output.set(m_outputGeomCol.ordinal(), null);
			return true;
		}
	}
	
	protected Geometry handleNullEmptyGeometry(Geometry geom) {
		if ( geom == null ) {
			return null;
		}
		else if ( geom.isEmpty() ) {
			return m_inputGeomType.newInstance();
		}
		else {
			throw new AssertionError("Should not be called: " + getClass());
		}
	}
}
