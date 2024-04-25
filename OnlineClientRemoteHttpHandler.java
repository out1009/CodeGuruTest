package com.tm.online;

import java.net.InetSocketAddress;

import com.tm.bootstrap.NettyClientBootstrap;
import com.tm.constant.ConstantFactory;
import com.tm.db.app.vo.ChannelVO;
import com.tm.logging.LoggingFormat;
import com.tm.online.message.HttpRequestMessage;
import com.tm.online.message.MessageConvert;
import com.tm.utils.JsonUtil;
import com.tm.utils.TextUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *  로컬로 데이터를 수신하기 위한 서버 모듈 핸들러
 * 
 * <p>lombok 어노테이션 사용으로 생성자 및 Getter/Setter 메서드가 자동으로 생성되어 java 도큐먼트에 표현되지 않습니다.</p>
 * 
 * @author		: shin
 * @version		: 2.0.0
 */
@Slf4j
@RequiredArgsConstructor
@Sharable
public class OnlineClientRemoteHttpHandler extends SimpleChannelInboundHandler<Object> {
	/** 채널 정보 객체 **/
	private final ChannelVO channelVO;

	/** 송신 채널 */
	private Channel outboundChannel; 
	
	/**
	 * 채널이 접속되자마자 실행할 코드를 정의
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
		//무시아이피 체크
		if(ConstantFactory.isIgnoreIp(channelVO.getChnlId(), socketAddress.getAddress().getHostAddress())) {
			log.debug(LoggingFormat.formatSimple(channelVO.getChnlId(), "System is ignore. remote address", ctx.channel().remoteAddress()));
			ctx.close();
			return;
		}
		//허용아이피 체크
		if(!ConstantFactory.isAllowedIp(channelVO.getChnlId(), socketAddress.getAddress().getHostAddress())) {
			log.info(LoggingFormat.formatSimple(channelVO.getChnlId(), "System is not allowed. remote address", ctx.channel().remoteAddress()));
			ctx.close();
			return;
		}		
		
		ChannelManager.setRcvChannelMap(channelVO.getChnlId(), ctx.channel());
		log.info(LoggingFormat.formatSimple(channelVO.getChnlId(), "OPEN", ctx.channel().remoteAddress()));	
	}
	
	/**
	 * 채널을 읽을 때 동작할 코드를 정의
	 */
	@Override
	public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		if(msg instanceof ByteBuf) {
			final byte[] bytearray = ByteBufUtil.getBytes(((ByteBuf) msg));
			final String tlgrmId = TextUtil.substringB(bytearray, channelVO.getTlgrmIdSrtPt(), channelVO.getTlgrmIdLen(), channelVO.getLocalCharset());
			final String MsgKey = TextUtil.substringB(bytearray, channelVO.getMsgKeySrtPt(), channelVO.getMsgKeyLen(), channelVO.getLocalCharset());

			if(outboundChannel == null || !(outboundChannel.isActive())) {
				NettyClientBootstrap client = new NettyClientBootstrap();
				client.setWorkerGroup(ctx.channel().eventLoop());
				client.setInboundChannel(ctx.channel());
				client.setChannelVO(channelVO);
				final ChannelFuture channelFuture = client.init(1).connect(channelVO.getRemoteIp(), channelVO.getRemotePort());
				channelFuture.addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if (future.isSuccess()) {
								outboundChannel = future.channel();
								//byte[] -> LinkedHashMap -> Json -> ByteBuf
								outboundChannel.writeAndFlush(HttpRequestMessage.request(Unpooled.wrappedBuffer(JsonUtil.convertPojoToJson(MessageConvert.messageToJson(channelVO.getChnlId(), tlgrmId, bytearray, channelVO.getDataPfxLen(), channelVO.getLocalCharset())).getBytes(channelVO.getRemoteCharset())), channelVO.getHttpPath(), tlgrmId, MsgKey));
						} else {
							if (future.cause() != null) {
								log.error(LoggingFormat.formatSimple(channelVO.getChnlId(), "CONNECT FAIL", future.cause()));
							}
						}
					}
				});
			} else {
				//byte[] -> LinkedHashMap -> Json -> ByteBuf
				outboundChannel.writeAndFlush(HttpRequestMessage.request(Unpooled.wrappedBuffer(JsonUtil.convertPojoToJson(MessageConvert.messageToJson(channelVO.getChnlId(), tlgrmId, bytearray, channelVO.getDataPfxLen(), channelVO.getLocalCharset())).getBytes(channelVO.getRemoteCharset())), channelVO.getHttpPath(), tlgrmId, MsgKey));
			}
		} else if(msg instanceof FullHttpRequest) {
			
			final byte[] bytearray = ByteBufUtil.getBytes(((FullHttpRequest) msg).content());
			final String tlgrmId = ((FullHttpRequest) msg).headers().get("telegram-id");
			final String MsgKey = ((FullHttpRequest) msg).headers().get("message-key");
			if(outboundChannel == null || !(outboundChannel.isActive())) {
				NettyClientBootstrap client = new NettyClientBootstrap();
				client.setWorkerGroup(ctx.channel().eventLoop());
				client.setInboundChannel(ctx.channel());
				client.setChannelVO(channelVO);
				final ChannelFuture channelFuture = client.init(1).connect(channelVO.getRemoteIp(), channelVO.getRemotePort());

				channelFuture.addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if (future.isSuccess()) {
								outboundChannel = future.channel();
								outboundChannel.writeAndFlush(HttpRequestMessage.request(Unpooled.wrappedBuffer(bytearray), channelVO.getHttpPath(), tlgrmId, MsgKey));
						} else {
							if (future.cause() != null) {
								log.error(LoggingFormat.formatSimple(channelVO.getChnlId(), "CONNECT FAIL", future.cause()));
							}
						}
					}
				}); 
			} else {
				outboundChannel.writeAndFlush(HttpRequestMessage.request(Unpooled.wrappedBuffer(bytearray), channelVO.getHttpPath(), tlgrmId, MsgKey));
			}
		} 
	}	

	/**
	 * 예외가 발생할 때 동작할 코드를 정의
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		log.error(LoggingFormat.formatSimple(channelVO.getChnlId(), "EXCEPTION", cause), cause);
		ctx.close(); 
	}
	
	/**
	 * 채널이 활성 상태에서 벗어나 연결이 해제될 때 동작할 코드를 정의
	 */
	@Override
	public void channelInactive(final ChannelHandlerContext ctx) {
		if(ChannelManager.isRcvChannelMap(channelVO.getChnlId(), ctx.channel())) {
			ChannelManager.removeRcvChannelMap(channelVO.getChnlId(), ctx.channel());
			log.info(LoggingFormat.formatSimple(channelVO.getChnlId(), "CLOSE", ctx.channel().remoteAddress()));
		}
	}
}