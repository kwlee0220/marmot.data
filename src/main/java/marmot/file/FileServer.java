package marmot.file;

import java.io.IOException;
import java.io.InputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface FileServer {
	public InputStream readFile(String path) throws IOException;
	public long writeFile(String path, InputStream is) throws IOException;
	public boolean deleteFile(String path);
}
