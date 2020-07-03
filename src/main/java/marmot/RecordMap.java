package marmot;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordMap implements Map<String,Object> {
	private final Record m_record;
	private boolean m_readonly = false;
	
	public RecordMap(Record record) {
		m_record = record;
	}
	
	public RecordMap readonly(boolean flag) {
		m_readonly = flag;
		return this;
	}

	@Override
	public boolean isEmpty() {
		return m_record.getColumnCount() == 0;
	}
	
	@Override
    public int size() {
        return m_record.getColumnCount();
    }
	
	@Override
    public Object get(Object name) {
    	return m_record.get((String)name);
    }
	
	@Override
    public Object put(String key, Object value) {
		if ( !m_readonly ) {
			m_record.set(key, value);
			return value;
		}
		else {
			throw new IllegalStateException("cannot update record");
		}
	}

	@Override
	public boolean containsKey(Object key) {
		return m_record.existsColumn((String)key);
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}
    
	@Override
	public Set<Entry<String, Object>> entrySet() {
		 return m_record.fstream().toMap().entrySet();
	}

	@Override
	public Object remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		if ( !m_readonly ) {
			for ( Entry<? extends String,? extends Object> ent: m.entrySet() ) {
				try {
					m_record.set(ent.getKey(), ent.getValue());
				}
				catch ( ColumnNotFoundException ignored ) { }
			}
		}
		else {
			throw new IllegalStateException("cannot update record");
		}
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> keySet() {
		return m_record.getRecordSchema()
						.streamColumns()
						.map(Column::name)
						.toSet();
	}

	@Override
	public Collection<Object> values() {
		return Arrays.asList(m_record.getAll());
	}
	
	@Override
	public String toString() {
		return m_record.fstream()
						.map(kv -> "" + kv.key() + "=" + kv.value())
						.join(", ", "{", "}");
	}
}
