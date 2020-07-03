package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StringType extends PrimitiveDataType implements ComparableDataType, Comparator<String> {
	private static final long serialVersionUID = 1L;
	private static final StringType TYPE = new StringType();
	
	public static StringType get() {
		return TYPE;
	}
	
	private StringType() {
		super(TypeClass.STRING, String.class);
	}

	@Override
	public String newInstance() {
		return "";
	}
	
	@Override
	public String parseInstance(String str) {
		return str;
	}

	@Override
	public int compare(String v1, String v2) {
		if ( v1 != null && v2 != null ) {
			return v1.compareTo(v2);
		}
		else if ( v1 != null ) {
			return 1;
		}
		else if ( v2 != null ) {
			return -1;
		}
		else {
			return 1;
		}
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof String ) {
			oos.writeUTF((String)obj);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return ois.readUTF();
	}
}
