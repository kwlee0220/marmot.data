package marmot.remote.client;

import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import marmot.MarmotRuntime;
import marmot.file.FileServer;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcMarmotRuntimeProxy implements MarmotRuntime, AutoCloseable {
	private ManagedChannel m_channel;
	private GrpcDataSetServerProxy m_dsServer;
	private GrpcFileServerProxy m_fileServer;
	
	public static GrpcMarmotRuntimeProxy connect(String host, int port) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
														.usePlaintext()
														.build();
		return new GrpcMarmotRuntimeProxy(channel);
	}
	
	public GrpcMarmotRuntimeProxy(ManagedChannel channel) {
		m_channel = channel;
		m_dsServer = new GrpcDataSetServerProxy(channel);
		m_fileServer = new GrpcFileServerProxy(channel);
	}

	@Override
	public void close() {
		m_channel.shutdown();
		Try.run(() -> m_channel.awaitTermination(1, TimeUnit.SECONDS));
	}

	@Override
	public GrpcDataSetServerProxy getDataSetServer() {
		return m_dsServer;
	}

	@Override
	public FileServer getFileServer() {
		return m_fileServer;
	}
}
