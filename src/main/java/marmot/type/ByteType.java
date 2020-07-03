package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ByteType extends PrimitiveDataType implements IntegralDataType, Comparator<Byte> {
	private static final long serialVersionUID = 1L;
	private static final ByteType TYPE = new ByteType();
	
	public static ByteType get() {
		return TYPE;
	}
	
	private ByteType() {
		super(TypeClass.BYTE, Byte.class);
	}
	
	@Override
	public Byte newInstance() {
		return new Byte((byte)0);
	}
	
	@Override
	public Byte parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Byte.parseByte(str) : null;
	}

	@Override
	public int compare(Byte v1, Byte v2) {
		return Byte.compare(v1, v2);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Byte ) {
			oos.writeByte((Byte)obj);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return ois.readByte();
	}
}
