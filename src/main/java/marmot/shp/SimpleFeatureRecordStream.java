package marmot.shp;

import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStreamException;
import marmot.stream.AbstractRecordStream;
import marmot.type.GeometryDataType;
import utils.Utilities;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SimpleFeatureRecordStream extends AbstractRecordStream {
	private final FeatureIterator<SimpleFeature> m_reader;
	private RecordSchema m_schema;
	
	private SimpleFeature m_first = null;
	
	SimpleFeatureRecordStream(FeatureIterator<SimpleFeature> iter) {
		Utilities.checkNotNullArgument(iter);
		Utilities.checkArgument(iter.hasNext(), "FeatureIterator is empty");
		
		m_first = iter.next();
		SimpleFeatureType sfType = m_first.getFeatureType();
		
		try {
			m_schema = ShapefileDataSets.toRecordSchema(sfType);
		}
		catch ( Throwable e ) {
			throw new RecordStreamException("fails to read SimpleFeatureCollection: type="
											+ sfType + ", cause=" + e);
		}

		m_reader = iter;
		setLogger(LoggerFactory.getLogger(SimpleFeatureRecordStream.class));
	}
	
	SimpleFeatureRecordStream(SimpleFeatureType sfType, FeatureIterator<SimpleFeature> iter) {
		Utilities.checkNotNullArgument(sfType);
		Utilities.checkNotNullArgument(iter);
		
		try {
			m_schema = ShapefileDataSets.toRecordSchema(sfType);
		}
		catch ( Throwable e ) {
			throw new RecordStreamException("fails to read SimpleFeatureCollection: type="
											+ sfType + ", cause=" + e);
		}

		m_reader = iter;
		setLogger(LoggerFactory.getLogger(SimpleFeatureRecordStream.class));
	}
	
	SimpleFeatureRecordStream(FeatureCollection<SimpleFeatureType,SimpleFeature> sfColl)
		throws FactoryException  {
		this(sfColl.getSchema(), sfColl.features());
	}
	
	@Override
	protected void closeInGuard() {
		Try.run(m_reader::close);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public Record next() {
		if ( !m_reader.hasNext() ) {
			return null;
		}
		
		SimpleFeature feature;
		if ( m_first != null ) {
			feature = m_first;
			m_first = null;
		}
		else {
			feature = m_reader.next();
		}
		
		return new SimpleFeatureRecord(m_schema, feature);
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		if ( !m_reader.hasNext() ) {
			return null;
		}
		
		SimpleFeature feature;
		if ( m_first != null ) {
			feature = m_first;
			m_first = null;
		}
		else {
			feature = m_reader.next();
		}
		
		return new SimpleFeatureRecord(m_schema, feature);
	}
}