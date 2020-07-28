package marmot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import marmot.dataset.LfsAvroDataSetServer;
import marmot.file.LfsFileServer;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotLfsServer implements MarmotRuntime, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final String m_jdbcStr;
	private final File m_root;
	private final LfsAvroDataSetServer m_dsServer;
	private final LfsFileServer m_fileServer;
	
	public MarmotLfsServer(String jdbcStr, File root) {
		m_jdbcStr = jdbcStr;
		m_root = root;
		
		JdbcProcessor jdbc = JdbcProcessor.parseString(jdbcStr);
		m_dsServer = new LfsAvroDataSetServer(jdbc, m_root);
		m_fileServer = new LfsFileServer(m_root);
	}
	
	public static MarmotLfsServer format(String jdbcStr, File root) throws IOException {
		JdbcProcessor jdbc = JdbcProcessor.parseString(jdbcStr);
		LfsAvroDataSetServer.format(jdbc, root);
		
		return new MarmotLfsServer(jdbcStr, root);
	}

	@Override
	public LfsAvroDataSetServer getDataSetServer() {
		return m_dsServer;
	}

	@Override
	public LfsFileServer getFileServer() {
		return m_fileServer;
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		private final byte[] m_confBytes;
		
		private SerializationProxy(MarmotLfsServer marmot) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try ( ObjectOutputStream out = new ObjectOutputStream(baos) ) {
				out.writeUTF(marmot.m_jdbcStr);
				out.writeUTF(marmot.m_root.getAbsolutePath());
			}
			catch ( IOException nerverHappens ) { }
			m_confBytes = baos.toByteArray();
		}
		
		private Object readResolve() {
			try ( ObjectInputStream dis = new ObjectInputStream(new ByteArrayInputStream(m_confBytes)) ) {
				String jdbcStr = dis.readUTF();
				File datasetRoot = new File(dis.readUTF());
				
				return new MarmotLfsServer(jdbcStr, datasetRoot);
			}
			catch ( Exception e ) {
				throw new RuntimeException(e);
			}
		}
	}
}
