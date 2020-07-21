package marmot.remote.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import marmot.MarmotRuntime;
import marmot.dataset.DataSetServer;
import marmot.file.FileServer;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcMarmotRuntimeProxy implements MarmotRuntime {
	private GrpcDataSetServerProxy m_dsServer;
	private GrpcFileServerProxy m_fileServer;
	
	public static GrpcMarmotRuntimeProxy connect(String host, int port) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
														.usePlaintext()
														.build();
		return new GrpcMarmotRuntimeProxy(channel);
	}
	
	public GrpcMarmotRuntimeProxy(ManagedChannel channel) {
		m_dsServer = new GrpcDataSetServerProxy(channel);
		m_fileServer = new GrpcFileServerProxy(channel);
	}

	@Override
	public DataSetServer getDataSetServer() {
		return m_dsServer;
	}

	@Override
	public FileServer getFileServer() {
		return m_fileServer;
	}
}
