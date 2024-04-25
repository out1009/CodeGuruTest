package com.tm.online;

import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

import com.tm.db.app.vo.ChannelVO;
import com.tm.service.RedisService;
import com.tm.utils.BeanUtil;
import com.tm.utils.TextUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 온라인 클라이언트 타임아웃 모듈 핸들러
 * 
 * <p>lombok 어노테이션 사용으로 생성자 및 Getter/Setter 메서드가 자동으로 생성되어 java 도큐먼트에 표현되지 않습니다.</p>
 * 
 * @author		: Trionsoft
 * @version		: 2.0.0
 */
@Slf4j
@RequiredArgsConstructor
@Sharable
public class OnlineClientTimeoutHandler extends ChannelDuplexHandler {
	/** 채널 정보 객체 */
	private final ChannelVO channelVO;
	
	/** redis 서비스 */
	private RedisService redisService = (RedisService) BeanUtil.getBean("redisService");

	/** 재송신 스케줄 정보 */
	private final LinkedHashMap<String, ScheduledFuture<?>> tmTask = new LinkedHashMap<>();

	/** 재송신 스케줄 정보 */
	private final LinkedHashMap<String, Integer> tmTaskCnt = new LinkedHashMap<>();

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof ByteBuf) {
			if (channelVO.getRcvTimeout() > 0) {
				final byte[] bytearray = ByteBufUtil.getBytes(((ByteBuf) msg));
				final String MsgKey = TextUtil.substringB(bytearray, channelVO.getMsgKeySrtPt(), channelVO.getMsgKeyLen(), channelVO.getRemoteCharset());
				destroy(TextUtil.concat(channelVO.getChnlId(), MsgKey));
			}
		}
		super.channelRead(ctx, msg);
	}
	
	@Override
	public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		//핸들러가 읽기 시간 초과 이벤트에 대해서만 구성된 경우 void promise로 쓰기를 허용합니다.
		if (msg instanceof ByteBuf) {
			if (channelVO.getRcvTimeout() > 0) {
				final byte[] bytearray = ByteBufUtil.getBytes(((ByteBuf) msg));
				final String MsgKey = TextUtil.substringB(bytearray, channelVO.getMsgKeySrtPt(), channelVO.getMsgKeyLen(), channelVO.getRemoteCharset());

				ChannelPromise unvoid = promise.unvoid();
				ctx.write(msg, unvoid).addListener(new ChannelFutureListener() {
					public void operationComplete(ChannelFuture future) throws Exception {
						initialize(ctx, TextUtil.concat(channelVO.getChnlId(), MsgKey), bytearray);
					}
				});
			} else {
				ctx.write(msg, promise);
			}
		} else {
			ctx.write(msg, promise);
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		destroy();
		super.exceptionCaught(ctx, cause); 
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		destroy();
		super.channelInactive(ctx);
	}
	
	/**
	 * 타임아웃 초기화 세팅
	 * @param ctx 송신채널
	 * @param key 키정보
	 * @param message 메시지 객체
	 */
	private void initialize(ChannelHandlerContext ctx, String key, Object message) {
		int cnt = cnt(key);
		destroy(key);
		long timeoutNanoSeconds = Math.max(TimeUnit.SECONDS.toNanos(channelVO.getRcvTimeout()), TimeUnit.MILLISECONDS.toNanos(1));
		EventExecutor loop = ctx.channel().eventLoop();
		ScheduledFuture<?> future = loop.schedule(new ClientTimeoutTask(ctx, key, message), timeoutNanoSeconds, TimeUnit.NANOSECONDS);
		tmTask.put(key, future);
		tmTaskCnt.put(key, cnt);
	}
	
	/**
	 * 타입아웃 재전송 스레드 
	 */
	@RequiredArgsConstructor
	private final class ClientTimeoutTask implements Runnable {
		/** 채널 Context 객체 */
		private final ChannelHandlerContext ctx;
		/** 키정보 */
		private final String key;
		/** 메시지 객체 */
		private final Object message;
	
		/**
		 * 스레드 시작
		 */
		public void run() {
			//전송 건수 값 체크
			if(redisService.nullString(key)) {
				//전송 건수 값
				int cnt = tmTaskCnt.get(key);
				if( cnt < channelVO.getReTryCnt()) {
					cnt++;
					tmTaskCnt.put(key, cnt);
					//이벤트 호출
					ctx.fireUserEventTriggered(message);
				} else {
					log.error("[Channel: {}] SND TIMEOUT: {}", channelVO.getChnlId(), new String((byte[]) message, channelVO.getRemoteCharset()));
					//redis 삭제
					redisService.delRedis(key);
					destroy(key);
				}
			} else {
				destroy(key);
			}
		}
	}
	
	/**
	 * 타입아웃 재전송 횟구 조회 
	 * @param key 키정보
	 * @return 횟수
	 */
	public int cnt(String key) {
		if (tmTaskCnt.containsKey(key)) {
			return tmTaskCnt.get(key);
		} else {
			return 0;
		}
	}
	
	/**
	 * 타입아웃 재전송 스레드 종료 
	 * @param key 키정보
	 */
	public void destroy(String key) {
		if (tmTask.containsKey(key)) {
			tmTask.get(key).cancel(true);
			tmTask.remove(key);
			tmTaskCnt.remove(key);
		}
	}
	
	/**
	 * 타입아웃 전체 스레드 종료 
	 */
	public void destroy() {
		for (String key : tmTask.keySet()) {
			tmTask.get(key).cancel(true);
		}
	}
}
