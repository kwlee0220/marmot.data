package marmot;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;

import marmot.type.DataType;
import marmot.type.GeometryDataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Column implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final String m_name;
	private final DataType m_type;
	private final short m_ordinal;

	public Column(String name, DataType type) {
		Utilities.checkNotNullArgument(name, "column name is null");
		Utilities.checkNotNullArgument(type, "column type is null");

		m_name = name;
		m_type = type;
		m_ordinal = -1;
	}
	
	public Column(String name, DataType type, int ordinal) {
		Utilities.checkNotNullArgument(name, "column name");
		Utilities.checkNotNullArgument(type, "column type");
		
		m_name = name;
		m_type = type;
		m_ordinal = (short)ordinal;
	}
	
	/**
	 * 컬럼 이름을 반환한다.
	 * 
	 * @return	컬럼 이름.
	 */
	public String name() {
		return m_name;
	}
	
	/**
	 * 컬럼 타입을 반환한다.
	 * 
	 * @return	컬럼 타입 객체.
	 */
	public DataType type() {
		return m_type;
	}
	
	/**
	 * 컬머의 정의 순번을 반환한다.
	 * 
	 * @return	순번.
	 */
	public int ordinal() {
		return m_ordinal;
	}
	
	public String toStringExpr() {
		return m_name + ":" + ((m_type != null) ?m_type : "?");
	}
	
	public boolean matches(String name) {
		return m_name.matches(name);
	}
	
	@Override
	public String toString() {
		return m_name + ":" + ((m_type != null) ?m_type : "?");
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		if ( obj == null || !getClass().equals(obj.getClass()) ) {
			return false;
		}
		
		Column other = (Column)obj;
		return m_name.equals(other.m_name) && m_type.equals(other.m_type);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_name, m_type);
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final String m_name;
		private final DataType m_type;
		private final int m_ordinal;
		
		private SerializationProxy(Column col) {
			m_name = col.name();
			m_type = col.type();
			m_ordinal = col.ordinal();
		}
		
		private Object readResolve() {
			return new Column(m_name, m_type, m_ordinal);
		}
	}
}