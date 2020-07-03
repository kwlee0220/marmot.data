package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntType extends PrimitiveDataType implements IntegralDataType, Comparator<Integer> {
	private static final long serialVersionUID = 1L;
	private static final IntType TYPE = new IntType();
	
	public static IntType get() {
		return TYPE;
	}
	
	private IntType() {
		super(TypeClass.INT, Integer.class);
	}
	
	@Override
	public Integer newInstance() {
		return new Integer(0);
	}
	
	@Override
	public Integer parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Integer.parseInt(str) : null;
	}

	@Override
	public int compare(Integer v1, Integer v2) {
		return Integer.compare(v1, v2);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Integer ) {
			oos.writeInt((Integer)obj);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return ois.readInt();
	}
}
