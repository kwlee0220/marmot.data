package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.utils.Lists;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ListType extends DataType {
	private static final long serialVersionUID = 1L;
	static ListType NULL = new ListType(DataType.NULL);
	
	private final DataType m_elmType;
	private final String m_displayName;
	
	ListType(DataType elmType) {
		super(String.format("[%s]", elmType.id()), TypeClass.LIST, List.class);
		
		m_elmType = elmType;
		m_displayName = String.format("list[%s]", elmType.displayName());
	}

	@Override
	public String displayName() {
		return m_displayName;
	}

	@Override
	public Object newInstance() {
		return Lists.newArrayList();
	}

	@Override
	public Object parseInstance(String str) {
		throw new UnsupportedOperationException("type=" + this + ", str=" + str);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof List ) {
			List objList = (List)obj;
			
			oos.writeInt(objList.size());
			for ( Object elm: objList ) {
				m_elmType.serialize(elm, oos);
			}
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		int size = ois.readInt();
		List<Object> objList = new ArrayList<>();
		for ( int i =0; i < size; ++i ) {
			Object elm = m_elmType.deserialize(ois);
			objList.add(elm);
		}
		
		return objList;
	}
}
