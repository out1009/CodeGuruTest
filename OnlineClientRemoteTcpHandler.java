package com.tm.online;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import com.tm.constant.ConstantFactory;
import com.tm.db.app.vo.ChannelVO;
import com.tm.logging.LoggingFormat;
import com.tm.online.message.MessageConvert;
import com.tm.service.RedisService;
import com.tm.utils.BeanUtil;
import com.tm.utils.JsonUtil;
import com.tm.utils.TextUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
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
public class OnlineClientRemoteTcpHandler extends SimpleChannelInboundHandler<Object> {
	/** 채널 정보 객체 **/
	private final ChannelVO channelVO;

	/** 송신 채널 */
	private final Channel outboundChannel; 
	
	/** redis 서비스 */
	private RedisService redisService = (RedisService) BeanUtil.getBean("redisService");

	
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
			//1. redis 등록
			redisService.setRedis(TextUtil.concat(channelVO.getChnlId(), MsgKey), ctx.channel().id().asLongText());
			//2. 메시지 전송
			outboundChannel.writeAndFlush(Unpooled.wrappedBuffer(bytearray));
		} else if(msg instanceof FullHttpRequest) {
			final byte[] bytearray = ByteBufUtil.getBytes(((FullHttpRequest) msg).content());
			final String tlgrmId = ((FullHttpRequest) msg).headers().get("telegram-id");
			final String MsgKey = ((FullHttpRequest) msg).headers().get("message-key");
			//1. redis 등록
			redisService.setRedis(TextUtil.concat(channelVO.getChnlId(), MsgKey), ctx.channel().id().asLongText());
			//2. 메시지 변환 후 전송
			//byte[] -> Json -> LinkedHashMap -> String -> ByteBuf
			outboundChannel.writeAndFlush(Unpooled.wrappedBuffer(MessageConvert.jsonToMessageCombine(channelVO.getChnlId(), tlgrmId, JsonUtil.convertJsonToLinkedHashMap(new String(bytearray, Charset.forName("utf-8"))), channelVO.getDataPfxLen(), channelVO.getRemoteCharset()).getBytes(channelVO.getRemoteCharset())));
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