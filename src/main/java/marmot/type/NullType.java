package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NullType extends PrimitiveDataType {
	private static final long serialVersionUID = 1L;
	private static final NullType TYPE = new NullType();
	
	private NullType() {
		super(TypeClass.NULL, Void.class);
	}
	
	public static NullType get() {
		return TYPE;
	}
	
	@Override
	public Byte newInstance() {
		throw new AssertionError("should not be called");
	}
	
	@Override
	public Byte parseInstance(String str) {
		throw new AssertionError("should not be called");
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return null;
	}
}
