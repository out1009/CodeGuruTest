package com.tm.online;

import com.tm.bootstrap.NettyClientBootstrap;
import com.tm.bootstrap.NettyServerBootstrap;
import com.tm.db.app.vo.ChannelVO;
import com.tm.logging.LoggingFormat;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 로컬로 데이터를 수신하여 목적지로 송신하기 위한 모듈
 * 
 * <p>lombok 어노테이션 사용으로 생성자 및 Getter/Setter 메서드가 자동으로 생성되어 java 도큐먼트에 표현되지 않습니다.</p>
 * 
 * @author		: shin
 * @version		: 2.0.0
 * @see com.tm.online.OnlineProxyRcvHandler
 * @see com.tm.online.OnlineProxySndHandler
 */
@Slf4j
@RequiredArgsConstructor
public class OnlineClient {
	/** 채널 정보 객체 **/
	private final ChannelVO channelVO;
	
	/**
	 * 온라인 Proxy 동작 
	 * @throws Exception 오류 
	 */
	public void start() throws Exception {
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(channelVO.getSsnCnt() * 2);
		try {
			
			if("tcp".equals(channelVO.getRemotePrtcl())) {
				NettyClientBootstrap client = new NettyClientBootstrap();
				client.setWorkerGroup(workerGroup);
				client.setChannelVO(channelVO);
				//송신 채널 
				for(int i=1; i<=channelVO.getSsnCnt(); i++) {
					client.init(i).connect(channelVO.getRemoteIp(), channelVO.getRemotePort()).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (! future.isSuccess()) {
								if (future.cause() != null) {
									log.error(LoggingFormat.formatSimple(channelVO.getChnlId(), "CONNECT FAIL", future.cause()));
								}
							}
						}
					});
				}
			}
			NettyServerBootstrap server = new NettyServerBootstrap();
			server.setBossGroup(bossGroup);
			server.setWorkerGroup(workerGroup);
			server.setChannelVO(channelVO);
			
			final ChannelFuture f = server.init().bind(channelVO.getLocalPort()).sync();
			log.info( "[Channel: {}] Binding on port: {}", channelVO.getChnlId(), channelVO.getLocalPort());
			f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			log.info("[Channel: {}] Shutting down online server.", channelVO.getChnlId());
		} catch (Exception e) {
			throw e;
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}
}