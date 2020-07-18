package marmot.optor.geo;

import com.vividsolutions.jts.geom.Point;

import marmot.Column;
import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.RecordLevelTransformedStream;
import marmot.support.DataUtils;
import marmot.type.DataType;
import marmot.type.PointType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToGeometryPointReader implements RecordReader {
	private final RecordReader m_input;
	private final String m_xColName;
	private final String m_yColName;
	private final String m_outColName;
	private final String m_srid;
	
	private RecordSchema m_schema;
	private Column m_xCol;
	private Column m_yCol;
	private Column m_outCol;

	public ToGeometryPointReader(RecordReader input, String xColName, String yColName, String outColName, String srid) {
		Utilities.checkNotNullArgument(xColName, "x-column is null");
		Utilities.checkNotNullArgument(yColName, "y-column is null");
		Utilities.checkNotNullArgument(outColName, "output column is null");
		Utilities.checkNotNullArgument(srid, "SRID is null");
		
		m_input = input;
		m_xColName = xColName;
		m_yColName = yColName;
		m_outColName = outColName;
		m_srid = srid;
		
		initialize();
	}
	
	public Column getOutputGeometryColumn() {
		return m_outCol;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_input.read());
	}
	
	@Override
	public String toString() {
		return String.format("%s: (%s,%s)->%s(%s)", getClass().getSimpleName(),
								m_xColName, m_yColName, m_outColName, m_srid);
	}
	
	private class StreamImpl extends RecordLevelTransformedStream {
		StreamImpl(RecordStream inputStrm) {
			super(inputStrm);
		}

		@Override
		public RecordSchema getRecordSchema() {
			return ToGeometryPointReader.this.getRecordSchema();
		}

		@Override
		public boolean transform(Record input, Record output) {
			output.set(input);
			
			Object xObj = getValue(input, m_xCol);
			Object yObj = getValue(input, m_yCol);
			if ( xObj != null && yObj != null ) {
				try {
					double xpos = DataUtils.asDouble(xObj);
					double ypos = DataUtils.asDouble(yObj);
					Point pt = PointType.toPoint(xpos, ypos);
					output.set(m_outCol.ordinal(), pt);
				}
				catch ( Exception e ) {
					output.set(m_outCol.ordinal(), null);
					throw e;
				}
			}
			else {
				output.set(m_outCol.ordinal(), null);
			}
			
			return true;
		}
	}
	
	private Object getValue(Record input, Column col) {
		Object obj = input.get(col.ordinal());
		if ( obj instanceof String && ((String)obj).length() == 0 ) {
			return null;
		}
		else {
			return obj;
		}
	}

	private void initialize() {
		RecordSchema inputSchema = m_input.getRecordSchema();
		m_xCol = inputSchema.findColumn(m_xColName).getOrNull();
		if ( m_xCol == null ) {
			throw new IllegalArgumentException("unknown x-column: " + m_xColName
												+ ", schema=" + inputSchema);
		}
		switch ( m_xCol.type().typeClass() ) {
			case DOUBLE:
			case FLOAT:
			case INT:
			case SHORT:
			case STRING:
				break;
			default:
				throw new IllegalArgumentException("invalid x-column type: name="
													+ m_xColName + ", type=" + m_xCol.type());
			
		}
		
		m_yCol = inputSchema.findColumn(m_yColName).getOrNull();
		if ( m_yCol == null ) {
			throw new IllegalArgumentException("unknown y-column: " + m_yColName + ", schema="
												+ inputSchema);
		}
		switch ( m_yCol.type().typeClass() ) {
			case DOUBLE:
			case FLOAT:
			case INT:
			case SHORT:
			case STRING:
				break;
			default:
				throw new IllegalArgumentException("invalid y-column type: name="
													+ m_yColName + ", type=" + m_yCol.type());
			
		}
		
		m_schema = inputSchema.toBuilder()
								.addOrReplaceColumn(new Column(m_outColName, DataType.POINT(m_srid)))
								.build();
		m_outCol = m_schema.getColumn(m_outColName);
	}
}
