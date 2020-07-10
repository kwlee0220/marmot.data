package marmot.stream;

import com.vividsolutions.jts.geom.Envelope;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StatsCollectingRecordStream extends AbstractRecordStream {
	private final RecordStream m_stream;
	private long m_count;
	private int m_geomColIdx;
	private Envelope m_mbr;
	
	public StatsCollectingRecordStream(RecordStream stream) {
		m_stream = stream;
		m_count = 0;
		m_geomColIdx = stream.getRecordSchema()
							.findColumn("the_geom")
							.filter(col -> col.type().isGeometryType())
							.map(Column::ordinal)
							.getOrElse(-1);
		m_mbr = (m_geomColIdx >= 0) ? new Envelope() : null; 
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_stream.close();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_stream.getRecordSchema();
	}
	
	public long getRecordCount() {
		return m_count;
	}
	
	public Envelope getBounds() {
		return m_mbr;
	}

	@Override
	public Record next() {
		Record record = m_stream.next();
		if ( record != null ) {
			++m_count;
			if ( m_geomColIdx >= 0 ) {
				m_mbr.expandToInclude(record.getGeometry(m_geomColIdx).getEnvelopeInternal());
			}
		}
		
		return record;
	}
	
	@Override
	public Record nextCopy() {
		Record record = m_stream.nextCopy();
		if ( record != null ) {
			++m_count;
			if ( m_geomColIdx >= 0 ) {
				m_mbr.expandToInclude(record.getGeometry(m_geomColIdx).getEnvelopeInternal());
			}
		}
		
		return record;
	}
}
