package com.tm.online;

import java.util.concurrent.TimeUnit;

import com.tm.bootstrap.NettyClientBootstrap;
import com.tm.constant.ConstantFactory;
import com.tm.db.app.vo.ChannelVO;
import com.tm.db.app.vo.TelegramVO;
import com.tm.logging.LoggingFormat;
import com.tm.online.message.HttpResponseMessage;
import com.tm.online.message.MessageConvert;
import com.tm.service.RedisService;
import com.tm.utils.BeanUtil;
import com.tm.utils.JsonUtil;
import com.tm.utils.TextUtil;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
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
public class OnlineClientTcpSndHandler extends SimpleChannelInboundHandler<Object> {
	/** 채널 정보 객체 **/
	private final ChannelVO channelVO;
	/** 채널 순번 **/
	private final int chnlSeq;
	/** redis 서비스 */
	private RedisService redisService = (RedisService) BeanUtil.getBean("redisService");

	
	/**
	 * 채널이 접속되자마자 실행할 코드를 정의
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ChannelManager.setSndChannelMap(channelVO.getChnlId(), chnlSeq, ctx.channel());
		log.info(LoggingFormat.formatSimple(channelVO.getChnlId(), "CONNECT", ctx.channel().remoteAddress()));
	}
	
	/**
	 * 채널을 읽을 때 동작할 코드를 정의
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		if(msg instanceof ByteBuf) {
			final byte[] bytearray = ByteBufUtil.getBytes(((ByteBuf) msg));
			//idle 사용시
			if(channelVO.isIdleBln()) {
				if(bytearray.length == (channelVO.getIdleLen()+ channelVO.getDataPfxLen())) {
					return;
				}
			}
			String tlgrmId = TextUtil.substringB(bytearray, channelVO.getTlgrmIdSrtPt(), channelVO.getTlgrmIdLen(), channelVO.getRemoteCharset());
			String MsgKey = TextUtil.substringB(bytearray, channelVO.getMsgKeySrtPt(), channelVO.getMsgKeyLen(), channelVO.getRemoteCharset());
			//1. redis에 등록 여부 체크(전송시 등록)
			if(redisService.nullString(TextUtil.concat(channelVO.getChnlId(), MsgKey))) {
				//1.1. 양방향일시 만 응답 전송
				if(redisService.isRedis(TextUtil.concat(channelVO.getChnlId(), MsgKey))) {
					//1.1.1. redis를 이용하여 수신 받은 내부채널 조회
					final Channel channel = ChannelManager.getRcvChannel(redisService.getRedis(TextUtil.concat(channelVO.getChnlId(), MsgKey)));
					//1.1.2. 체널 체크
					if(channel != null && channel.isActive()) {
						//1.1.1.1. 내부 tcp 인경우
						if("tcp".equals(channelVO.getLocalPrtcl())) {
							channel.writeAndFlush(Unpooled.wrappedBuffer(bytearray));
						//1.1.1.2. 내부 http 인경우
						} else if("http".equals(channelVO.getLocalPrtcl())) {
							//byte[] -> LinkedHashMap -> Json -> ByteBuf
							channel.writeAndFlush(HttpResponseMessage.http200(Unpooled.wrappedBuffer(JsonUtil.convertPojoToJson(MessageConvert.messageToJson(channelVO.getChnlId(), tlgrmId, bytearray, channelVO.getDataPfxLen(), channelVO.getRemoteCharset())).getBytes(channelVO.getLocalCharset())), tlgrmId, MsgKey));
						}
					//1.1.3. 채널이 없거나 종료 되었을 시 
					} else {
						log.error(LoggingFormat.format(channelVO.getChnlId(), "Not connected to local channel. Message", bytearray, channelVO.getRemoteCharset()));
					}
				//1.2. 단방향 미응답 처리
				} else {
					log.error(LoggingFormat.format(channelVO.getChnlId(), "SND IGNR", bytearray, channelVO.getRemoteCharset()));
				}
				//1.3. redis 삭제
				redisService.delRedis(TextUtil.concat(channelVO.getChnlId(), MsgKey));
			//2. 기응답 or 타임아웃일 시
			} else {
				log.error(LoggingFormat.format(channelVO.getChnlId(), "SND SKIP", bytearray, channelVO.getRemoteCharset()));
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
		ChannelManager.removeSndChannelMap(channelVO.getChnlId(), chnlSeq);
		log.info(LoggingFormat.formatSimple(channelVO.getChnlId(), "DISCONNECT", ctx.channel().remoteAddress()));
	}
	
	/**
	 * 채널이 이벤트루프에서 등록 해제되고 입출력할수 없을 때 동작할 코드를 정의
	 */
	@Override
	public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
		if("tcp".equals(channelVO.getRemotePrtcl())) {
			EventLoop loop = ctx.channel().eventLoop();
			loop.schedule( new Runnable() {
				@Override
				public void run() {
					connect(loop);
				}
			}, channelVO.getReConnDelay(), TimeUnit.SECONDS );
		}
	}
	
	/**
	 * 이벤트가 호출 될 때 동작할 코드를 정의
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent e = (IdleStateEvent) evt;
			if (e.state() == IdleState.WRITER_IDLE) {
				ctx.writeAndFlush(getIdle().getBytes());
			}
		//재전송 이벤트
		} else if (evt instanceof byte[]) {
			ctx.writeAndFlush(Unpooled.wrappedBuffer((byte[]) evt));
		}
	}
	
	/**
	 * idle 메시지 반환
	 * @return idle 메시지
	 */
	public String getIdle() {
		StringBuilder sb = new StringBuilder();
		for(TelegramVO vo : ConstantFactory.tlgrmMap.get(TextUtil.concat(channelVO.getChnlId(), "I"))) {
			sb.append(TextUtil.pad(vo.getFieldType(), (String) vo.getDefVal(), vo.getFieldLen(), channelVO.getRemoteCharset()));
		}
		return TextUtil.concat(TextUtil.padLeft(sb.toString().getBytes(channelVO.getRemoteCharset()).length, channelVO.getDataPfxLen(), channelVO.getRemoteCharset()), sb.toString());
	}
	
	/**
	 * 클라이언트 재접속
	 * @param loop 이벤트루프
	 */
	public void connect(final EventLoop loop) {
		NettyClientBootstrap client = new NettyClientBootstrap();
		client.setWorkerGroup(loop);
		client.setChannelVO(channelVO);
		Bootstrap bootstrap = client.init(chnlSeq);
		bootstrap.connect(channelVO.getRemoteIp(), channelVO.getRemotePort()).addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (!future.isSuccess()) {
					if (future.cause() != null) {
						log.error(LoggingFormat.formatSimple(channelVO.getChnlId(), "CONNECT FAIL", future.cause()));
					}
				}
			}
		});
	}
}