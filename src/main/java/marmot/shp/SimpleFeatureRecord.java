package marmot.shp;

import java.util.List;

import org.opengis.feature.simple.SimpleFeature;

import marmot.Record;
import marmot.RecordSchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class SimpleFeatureRecord implements Record {
	private final RecordSchema m_schema;
	private final SimpleFeature m_feature;
	
	SimpleFeatureRecord(RecordSchema schema, SimpleFeature feature) {
		m_schema = schema;
		m_feature = feature;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public SimpleFeature getSimpleFeature() {
		return m_feature;
	}

	@Override
	public Object get(int index) {
		return m_feature.getAttribute(index);
	}

	@Override
	public Object[] getAll() {
		List<Object> vList = m_feature.getAttributes();
		return vList.toArray(new Object[vList.size()]);
	}

	@Override
	public Record set(int idx, Object value) {
		m_feature.setAttribute(idx, value);
		return this;
	}

	@Override
	public String toString() {
		return m_feature.toString();
	}
}
