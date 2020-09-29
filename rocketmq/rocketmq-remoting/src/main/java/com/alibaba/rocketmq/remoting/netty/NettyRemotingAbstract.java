/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.remoting.protocol.RequestCodeEnum;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.alibaba.rocketmq.remoting.common.ServiceThread;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingSysResponseCode;


/**
 * Server与Client公用抽象类
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public abstract class NettyRemotingAbstract {
    private static final Logger plog = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingAbstract.class);

    // 信号量，Oneway情况会使用，防止本地Netty缓存请求过多
    protected final Semaphore semaphoreOneway;

    // 信号量，异步调用情况会使用，防止本地Netty缓存请求过多
    protected final Semaphore semaphoreAsync;

    // 缓存所有对外请求
    protected final ConcurrentHashMap<Integer /* opaque */, ResponseFuture> responseTable =
            new ConcurrentHashMap<Integer, ResponseFuture>(256);

    // 默认请求代码处理器
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    // 注册的各个RPC处理器
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    protected final NettyEventExecuter nettyEventExecuter = new NettyEventExecuter();


    public abstract ChannelEventListener getChannelEventListener();


    public abstract RPCHook getRPCHook();


    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecuter.putNettyEvent(event);
    }

    class NettyEventExecuter extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int MaxSize = 10000;


        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= MaxSize) {
                this.eventQueue.add(event);
            } else {
                plog.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(),
                        event.toString());
            }
        }


        @Override
        public void run() {
            plog.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStoped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    plog.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            plog.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return NettyEventExecuter.class.getSimpleName();
        }
    }


    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }


    /**
     *  接收消息，处理请求数据：
     *  1.客户端注册和注销请求；
     *  2.拉取消息请求；
     *  3.发送消息请求；
     *  4.查询消息请求；
     *  5.nameServer相关请求；
     *
     * @param ctx
     * @param cmd
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());

        final Pair<NettyRequestProcessor, ExecutorService> pair =
                null == matched ? this.defaultRequestProcessor : matched;

        if (pair != null) {
            try {
                // 这里需要做流控，要求线程池对应的队列必须是有大小限制的
                pair.getObject2().submit(new ProcessRequestCommandTask(pair,ctx,cmd));
            } catch (RejectedExecutionException e) {
                // 每个线程10s打印一次
                if ((System.currentTimeMillis() % 10000) == 0) {
                    plog.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) //
                            + ", too many requests and system thread pool busy, RejectedExecutionException " //
                            + pair.getObject2().toString() //
                            + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response =
                            RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                                    "too many requests and system thread pool busy, please try another server");
                    response.setOpaque(cmd.getOpaque());
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                            error);
            response.setOpaque(cmd.getOpaque());
            ctx.writeAndFlush(response);
            plog.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }

    /**
     *  接收消息，处理请求数据task
     *
     */
    private class ProcessRequestCommandTask implements Runnable{

        private Pair<NettyRequestProcessor, ExecutorService> pair;

        private ChannelHandlerContext ctx;

        private RemotingCommand cmd;

        public ProcessRequestCommandTask(Pair<NettyRequestProcessor, ExecutorService> pair,ChannelHandlerContext ctx,RemotingCommand cmd){
            this.pair = pair;
            this.cmd = cmd;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            try {
                RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();
                String address = RemotingHelper.parseChannelRemoteAddr(ctx.channel());

                if (rpcHook != null) {
                    rpcHook.doBeforeRequest(address, cmd);
                }

                final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                if (rpcHook != null) {
                    rpcHook.doAfterResponse(address, cmd, response);
                }

                if (!cmd.isOnewayRPC()) {
                    if (response != null) {
                        response.setOpaque(cmd.getOpaque());
                        response.markResponseType();
                        try {
//                                    plog.info("-----[broker C]----- send message remote ip = {} , {}", address, response.getExtFields()!=null?response.getExtFields():"");
                            ctx.writeAndFlush(response);
                        } catch (Throwable e) {
                            plog.error("process request over, but response failed", e);
                            plog.error(cmd.toString());
                            plog.error(response.toString());
                        }
                    } else {
                        // 收到请求，但是没有返回应答，可能是processRequest中进行了应答，忽略这种情况
                    }
                }
            } catch (Throwable e) {
                plog.error("process request exception", e);
                plog.error(cmd.toString());

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response =
                            RemotingCommand.createResponseCommand(
                                    RemotingSysResponseCode.SYSTEM_ERROR,//
                                    RemotingHelper.exceptionSimpleDesc(e));
                    response.setOpaque(cmd.getOpaque());
                    ctx.writeAndFlush(response);
                }
            }
        }
    }


    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final ResponseFuture responseFuture = responseTable.get(cmd.getOpaque());
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);
            responseFuture.release();
            responseTable.remove(cmd.getOpaque());

            if (responseFuture.getInvokeCallback() != null) {
                boolean runInThisThread = false;
                ExecutorService executor = this.getCallbackExecutor();
                if (executor != null) {
                    try {
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    responseFuture.executeInvokeCallback();
                                } catch (Throwable e) {
                                    plog.warn("excute callback in executor exception, and callback throw", e);
                                }
                            }
                        });
                    } catch (Exception e) {
                        runInThisThread = true;
                        plog.warn("excute callback in executor exception, maybe executor busy", e);
                    }
                } else {
                    runInThisThread = true;
                }

                if (runInThisThread) {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        plog.warn("executeInvokeCallback Exception", e);
                    }
                }
            } else {
                responseFuture.putResponse(cmd);
            }
        } else {
            plog.warn("receive response, but not matched any request, "
                    + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            plog.warn(cmd.toString());
        }

    }


    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }


    abstract public ExecutorService getCallbackExecutor();


    public void scanResponseTable() {
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                it.remove();
                try {
                    rep.executeInvokeCallback();
                } catch (Throwable e) {
                    plog.warn("scanResponseTable, operationComplete Exception", e);
                } finally {
                    rep.release();
                }

                plog.warn("remove timeout request, " + rep);
            }
        }
    }

    /**
     *  发送消息，同步刷盘，SYNC_FLUSH
     *  ①写入pageCache后，线程等待；
     *  ②刷盘线程刷盘后，唤醒前端等待线程；
     *  ③前端等待线程向用户返回成功；
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
                                          final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException {
        try {
            final ResponseFuture responseFuture =
                    new ResponseFuture(request.getOpaque(), timeoutMillis, null, null);
            this.responseTable.put(request.getOpaque(), responseFuture);

            if (request != null && request.getBody() != null && request.getBody().length > 0) {
                String data = new String(request.getBody());
                if(StringUtils.isNotBlank(data)) {
                    plog.info("-----[broker sendMessage]----- nettyRemotingClient invokeSync operationComplete , remote address = {} , requestCode = {} , data = {}", channel.remoteAddress(), RequestCodeEnum.getAllMessageByValue(request.getCode()), data.substring(0, data.length() > 5000 ? 5000 : data.length()) + "..");
                }
            }

            plog.info("-----[broker A]----- send message remote ip = {} , {}",channel.remoteAddress(),request.getExtFields()!=null?request.getExtFields():"");

            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {

                    if (f.isSuccess()) {
//                        logger.info("invokeSyncImpl success! opaque = " + request.getOpaque());
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }

                    responseTable.remove(request.getOpaque());
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    logger.info("Sync send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    plog.info("Sync send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    plog.warn(request.toString());
                }
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseChannelRemoteAddr(channel),
                            timeoutMillis,"responseCommand is null!", responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel),
                            "responseCommand is null!",responseFuture.getCause());
                }
            }

            return responseCommand;
        } finally {
            this.responseTable.remove(request.getOpaque());
        }
    }

    /**
     *  异步刷盘，ASYNC_FLUSH
     *  ①写入pageCache后，向用户返回成功；
     *  ②刷盘线程刷盘；
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request,
                                final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

            final ResponseFuture responseFuture =
                    new ResponseFuture(request.getOpaque(), timeoutMillis, invokeCallback, once);
            this.responseTable.put(request.getOpaque(), responseFuture);
            try {
                plog.info("-----[broker Flush]----- send message remote ip = {} , {}",channel.remoteAddress(),request.getExtFields()!=null?request.getExtFields():"");

                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {

                            /*
                             2015-09-13 22:20:43,835 INFO RocketmqRemoting(142) - receive msg from server , brokerId = 0
2015-09-13 22:20:43,836 WARN RocketmqRemoting(439) - Async send a request command to channel </115.192.23.27:10911> true.
2015-09-13 22:20:43,836 WARN RocketmqRemoting(441) - RemotingCommand [code=11, language=JAVA, version=79, opaque=330, flag(B)=0, remark=null, extFields={topic=OPEN_PROJECT_TOPIC, suspendTimeoutMillis=15000, subVersion=1442153909403, queueId=0, consumerGroup=OPEN_PROJECT_C_GROUP, maxMsgNums=32, sysFlag=3, commitOffset=39, queueOffset=39}]
                             */
//                            plog.warn("Async send a request command to channel <" + channel.remoteAddress()
//                                    + "> true.");
//                            plog.warn(request.toString());

                            responseFuture.setSendRequestOK(true);
                            return;
                        } else {
                            responseFuture.setSendRequestOK(false);
                        }

                        responseFuture.putResponse(null);
                        responseTable.remove(request.getOpaque());
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            plog.warn("excute callback in writeAndFlush addListener, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }

                        plog.warn("Async send a request command to channel <{}> failed.",
                                RemotingHelper.parseChannelRemoteAddr(channel));
                        plog.warn(request.toString());
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                plog.warn(
                        "Async send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel)
                                + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String
                                .format(
                                        "invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                                        timeoutMillis,//
                                        this.semaphoreAsync.getQueueLength(),//
                                        this.semaphoreAsync.availablePermits()//
                                );
                plog.warn(info);
                plog.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }


    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request,
                                 final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException {
        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
//                plog.info("-----[broker F]----- send message remote ip = {} , ExtFields = {}", channel.remoteAddress(),request.getExtFields()!=null?request.getExtFields():"");

                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            plog.warn("Oneway send a request command to channel <" + channel.remoteAddress()
                                    + "> failed.");
                            plog.warn(request.toString());
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                plog.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info =
                        String
                                .format(
                                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                                        timeoutMillis,//
                                        this.semaphoreAsync.getQueueLength(),//
                                        this.semaphoreAsync.availablePermits()//
                                );
                plog.warn(info);
                plog.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }
}
