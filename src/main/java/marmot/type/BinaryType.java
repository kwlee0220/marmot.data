package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryType extends PrimitiveDataType {
	private static final long serialVersionUID = 1L;
	private static final BinaryType TYPE = new BinaryType();
	
	public static BinaryType get() {
		return TYPE;
	}
	
	private BinaryType() {
		super(TypeClass.BINARY, byte[].class);
	}

	@Override
	public byte[] newInstance() {
		return new byte[0];
	}
	
	@Override
	public Object parseInstance(String str) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof byte[] ) {
			byte[] bytes = (byte[])obj;
			oos.writeInt(bytes.length);
			oos.write(bytes);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		int nbytes = ois.readInt();
		byte[] bytes = new byte[nbytes];
		ois.readFully(bytes);
		
		return bytes;
	}
}
