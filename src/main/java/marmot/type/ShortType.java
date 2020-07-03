package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShortType extends PrimitiveDataType implements IntegralDataType, Comparator<Short> {
	private static final long serialVersionUID = 1L;
	private static final ShortType TYPE = new ShortType();
	
	public static ShortType get() {
		return TYPE;
	}
	
	private ShortType() {
		super(TypeClass.SHORT, Short.class);
	}
	
	@Override
	public Short newInstance() {
		return new Short((short)0);
	}
	
	@Override
	public Short parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Short.parseShort(str) : null;
	}

	@Override
	public int compare(Short v1, Short v2) {
		return Short.compare(v1, v2);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Short ) {
			oos.writeShort((Short)obj);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return ois.readShort();
	}
}
