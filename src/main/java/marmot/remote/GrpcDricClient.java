package marmot.remote;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import marmot.dataset.DataSetNotFoundException;
import marmot.remote.client.GrpcDataSetProxy;
import marmot.remote.client.GrpcDataSetServerProxy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDricClient {
	private final Server m_server;
	
	private final ManagedChannel m_channel;
	private final ExecutorService m_executor;
	private final GrpcDataSetServerProxy m_service;
	
	public static GrpcDricClient connect(String host, int port) throws IOException {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
													.usePlaintext()
													.build();
		
		return new GrpcDricClient(channel);
	}
	
	private GrpcDricClient(ManagedChannel channel) throws IOException {
		m_channel = channel;
		
		m_executor = Executors.newFixedThreadPool(16);
		m_service = new GrpcDataSetServerProxy(channel);

		m_server = ServerBuilder.forPort(0).build();
		m_server.start();
	}
	
	public Server getGrpcServer() {
		return m_server;
	}
	
	public void disconnect() {
		m_channel.shutdown();
		m_server.shutdown();
		m_executor.shutdown();
	}
	
	ManagedChannel getChannel() {
		return m_channel;
	}
	
	public GrpcDataSetServerProxy getStreamService() {
		return m_service;
	}
	
	public GrpcDataSetProxy getDataSet(String id) throws DataSetNotFoundException {
		return m_service.getDataSet(id);
	}

//	public InputStream download(String path) throws IOException {
//		return m_service.download(path);
//	}
//
//	public long upload(String path, InputStream stream) throws IOException {
//		return m_service.upload(path, stream);
//	}
}
