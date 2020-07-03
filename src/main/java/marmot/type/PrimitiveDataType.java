package marmot.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class PrimitiveDataType extends DataType {
	private static final long serialVersionUID = 1L;
	
	protected PrimitiveDataType(TypeClass tc, Class<?> instClass) {
		super(""+tc.get(), tc, instClass);
	}

	@Override
	public String displayName() {
		return typeClass().name();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		PrimitiveDataType other = (PrimitiveDataType)obj;
		return typeClass().equals(other.typeClass());
	}
	
	@Override
	public int hashCode() {
		return typeClass().hashCode();
	}
}
