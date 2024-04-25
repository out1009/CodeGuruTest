package com.tm.online;

import java.nio.charset.Charset;

import com.tm.db.app.vo.ChannelVO;
import com.tm.logging.LoggingFormat;
import com.tm.online.message.HttpResponseMessage;
import com.tm.online.message.MessageConvert;
import com.tm.utils.JsonUtil;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *  목적지로 송신하기 위한 클라이언트 모듈 핸들러
 * 
 * <p>lombok 어노테이션 사용으로 생성자 및 Getter/Setter 메서드가 자동으로 생성되어 java 도큐먼트에 표현되지 않습니다.</p>
 * 
 * @author		: shin
 * @version		: 2.0.0
 */
@Slf4j
@RequiredArgsConstructor
@Sharable
public class OnlineClientHttpSndHandler extends SimpleChannelInboundHandler<Object> {
	/** 채널 정보 객체 **/
	private final ChannelVO channelVO;
	/** 수신 채널 */
	private final Channel inboundChannel;
	
	/**
	 * 채널이 접속되자마자 실행할 코드를 정의
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ChannelManager.setSndChannelMap(channelVO.getChnlId(), 1, ctx.channel());
	}
	
	/**
	 * 채널을 읽을 때 동작할 코드를 정의
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		if(msg instanceof FullHttpResponse) {
			final byte[] bytearray = ByteBufUtil.getBytes(((FullHttpResponse) msg).content());
			String tlgrmId = ((FullHttpResponse) msg).headers().get("telegram-id");
			String MsgKey = ((FullHttpResponse) msg).headers().get("message-key");
			//1.1. 체널 체크
			if(inboundChannel != null && inboundChannel.isActive()) {
				//1.1.1. 내부 tcp 인경우
				if("tcp".equals(channelVO.getLocalPrtcl())) {
					//ByteBuf.toString -> Json -> LinkedHashMap -> String -> ByteBuf
					inboundChannel.writeAndFlush(Unpooled.wrappedBuffer(MessageConvert.jsonToMessageCombine(channelVO.getChnlId(), tlgrmId, JsonUtil.convertJsonToLinkedHashMap(new String(bytearray, Charset.forName("utf-8"))), channelVO.getDataPfxLen(), channelVO.getRemoteCharset()).getBytes(channelVO.getLocalCharset())));
				//1.1.2. 내부 http 인경우
				} else if("http".equals(channelVO.getLocalPrtcl())) {
					inboundChannel.writeAndFlush(HttpResponseMessage.http200(Unpooled.wrappedBuffer(bytearray), tlgrmId, MsgKey));
				}
			//1.2. 채널이 없거나 종료 되었을 시 
			} else {
				log.error(LoggingFormat.format(channelVO.getChnlId(), "Not connected to local channel. Message", bytearray, channelVO.getRemoteCharset()));
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
		inboundChannel.close(); 
	}
	
	/**
	 * 채널이 활성 상태에서 벗어나 연결이 해제될 때 동작할 코드를 정의
	 */
	@Override
	public void channelInactive(final ChannelHandlerContext ctx) {
		ChannelManager.removeSndChannelMap(channelVO.getChnlId(), 1);
	}
	
	/**
	 * 이벤트가 호출 될 때 동작할 코드를 정의
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent e = (IdleStateEvent) evt;
			if (e.state() == IdleState.WRITER_IDLE) {
				ctx.close();
			}
		}
	}
}