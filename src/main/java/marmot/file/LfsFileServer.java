package marmot.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;

import com.google.common.io.ByteStreams;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LfsFileServer implements FileServer {
	private final File m_root;
	
	public LfsFileServer(File root) {
		m_root = root;
	}

	@Override
	public InputStream readFile(String path) throws IOException {
		File fullPath = new File(m_root, path);
		return new FileInputStream(fullPath);
	}

	@Override
	public long writeFile(String path, InputStream is) throws IOException {
		File fullPath = new File(m_root, path);
		try ( FileOutputStream out = new FileOutputStream(fullPath) ) {
			return ByteStreams.copy(is, out);
		}
	}

	@Override
	public boolean deleteFile(String path) {
		File fullPath = new File(m_root, path);
		return FileUtils.deleteQuietly(fullPath);
	}
}
