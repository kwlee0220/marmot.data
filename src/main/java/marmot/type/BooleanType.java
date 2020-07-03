package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BooleanType extends PrimitiveDataType {
	private static final long serialVersionUID = 1L;
	private static final BooleanType TYPE = new BooleanType();
	
	public static BooleanType get() {
		return TYPE;
	}
	
	private BooleanType() {
		super(TypeClass.BOOLEAN, Boolean.class);
	}

	@Override
	public Boolean newInstance() {
		return false;
	}
	
	@Override
	public Boolean parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Boolean.parseBoolean(str) : null;
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Boolean ) {
			oos.writeBoolean((Boolean)obj);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return ois.readBoolean();
	}
}
