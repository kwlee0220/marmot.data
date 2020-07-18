package marmot;

import marmot.dataset.DataSetServer;
import marmot.file.FileServer;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotRuntime {
	public DataSetServer getDataSetServer();
	public FileServer getFileServer();
}
