/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.AuthCallbackHandler;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SaslClientAuthenticator implements Authenticator {

    public enum SaslState {
        SEND_HANDSHAKE_REQUEST, RECEIVE_HANDSHAKE_RESPONSE, INITIAL, INTERMEDIATE, COMPLETE, FAILED
    }

    private static final Logger LOG = LoggerFactory.getLogger(SaslClientAuthenticator.class);

    private final Subject subject; // 表示用于身份验证的主题
    private final String servicePrincipal;
    private final String host;
    private final String node;
    private final String mechanism;
    private final boolean handshakeRequestEnable;

    // assigned in `configure`
    private SaslClient saslClient; // javax.security包中提供用于salsl身份认证的客户端
    private Map<String, ?> configs;
    private String clientPrincipalName;
    private AuthCallbackHandler callbackHandler; // 用于收集身份认证信息的回调函数
    private TransportLayer transportLayer; // 表示底层的网络连接

    // buffers used in `authenticate`
    private NetworkReceive netInBuffer; // 读取身份认证信息的输入缓冲区
    private Send netOutBuffer; // 发送身份认证信息的输入缓冲区

    // Current SASL state
    private SaslState saslState; // 表示当前SaslClientAuthenticator状态
    // Next SASL state to be set when outgoing writes associated with the current SASL state complete
    private SaslState pendingSaslState; // 在输出缓冲区中的内容全部清空
    // Correlation ID for the next request
    private int correlationId;
    // Request header for which response from the server is pending
    private RequestHeader currentRequestHeader;

    public SaslClientAuthenticator(String node, Subject subject, String servicePrincipal, String host, String mechanism, boolean handshakeRequestEnable) throws IOException {
        this.node = node;
        this.subject = subject;
        this.host = host;
        this.servicePrincipal = servicePrincipal;
        this.mechanism = mechanism;
        this.handshakeRequestEnable = handshakeRequestEnable;
        this.correlationId = -1;
    }

    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs) throws KafkaException {
        try {
            this.transportLayer = transportLayer;
            this.configs = configs;

            // 初始化saslstate字段为SEND_HANDSHAKE_REQUEST
            setSaslState(handshakeRequestEnable ? SaslState.SEND_HANDSHAKE_REQUEST : SaslState.INITIAL);

            // determine client principal from subject for Kerberos to use as authorization id for the SaslClient.
            // For other mechanisms, the authenticated principal (username for PLAIN and SCRAM) is used as
            // authorization id. Hence the principal is not specified for creating the SaslClient.
            if (mechanism.equals(SaslConfigs.GSSAPI_MECHANISM))
                this.clientPrincipalName = firstPrincipal(subject);
            else
                this.clientPrincipalName = null;
            // 用于收集认证信息I的SaslClientCallbackHandler
            callbackHandler = new SaslClientCallbackHandler();
            callbackHandler.configure(configs, Mode.CLIENT, subject, mechanism);

            // 创建saslclient对象
            saslClient = createSaslClient();
        } catch (Exception e) {
            throw new KafkaException("Failed to configure SaslClientAuthenticator", e);
        }
    }

    private SaslClient createSaslClient() {
        try {
            return Subject.doAs(subject, new PrivilegedExceptionAction<SaslClient>() {
                public SaslClient run() throws SaslException {
                    String[] mechs = {mechanism};
                    LOG.debug("Creating SaslClient: client={};service={};serviceHostname={};mechs={}",
                        clientPrincipalName, servicePrincipal, host, Arrays.toString(mechs));
                    return Sasl.createSaslClient(mechs, clientPrincipalName, servicePrincipal, host, configs, callbackHandler);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new KafkaException("Failed to create SaslClient with mechanism " + mechanism, e.getCause());
        }
    }

    /**
     * Sends an empty message to the server to initiate the authentication process. It then evaluates server challenges
     * via `SaslClient.evaluateChallenge` and returns client responses until authentication succeeds or fails.
     *
     * The messages are sent and received as size delimited bytes that consists of a 4 byte network-ordered size N
     * followed by N bytes representing the opaque payload.
     */
    public void authenticate() throws IOException {
        // 发送缓冲区中还有未发送的数据，则需要将这些数据发送完毕
        if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps())
            return;

        switch (saslState) {
            // 处于SEND_HANDSHAKE_REQUESTh会创建SaslHandshakeRequest ，其中记录了当前客户端使用的sasl机制
            case SEND_HANDSHAKE_REQUEST:
                // When multiple versions of SASL_HANDSHAKE_REQUEST are to be supported,
                // API_VERSIONS_REQUEST must be sent prior to sending SASL_HANDSHAKE_REQUEST to
                // fetch supported versions.
                String clientId = (String) configs.get(CommonClientConfigs.CLIENT_ID_CONFIG);
                // 创建并发送SaslHandshakeRequest
                SaslHandshakeRequest handshakeRequest = new SaslHandshakeRequest(mechanism);
                currentRequestHeader = new RequestHeader(ApiKeys.SASL_HANDSHAKE.id,
                        handshakeRequest.version(), clientId, correlationId++);
                send(handshakeRequest.toSend(node, currentRequestHeader));
                setSaslState(SaslState.RECEIVE_HANDSHAKE_RESPONSE);
                break;
            // 客户端会接受服务端返回的SaslHands
            case RECEIVE_HANDSHAKE_RESPONSE:
                // 读取response
                byte[] responseBytes = receiveResponseOrToken();
                if (responseBytes == null)
                    break;
                else {
                    try {
                        // 解析SaslHandshakeResponse
                        handleKafkaResponse(currentRequestHeader, responseBytes);
                        currentRequestHeader = null;
                    } catch (Exception e) {
                        setSaslState(SaslState.FAILED);
                        throw e;
                    }
                    setSaslState(SaslState.INITIAL);
                    // Fall through and start SASL authentication using the configured client mechanism
                }
             // 会发送一个空的byte数组作为厨师的response发送给服务端
            case INITIAL:
                sendSaslToken(new byte[0], true);
                setSaslState(SaslState.INTERMEDIATE);
                break;
            // 此过程会读取服务端返回的Challenge
            case INTERMEDIATE:
                byte[] serverToken = receiveResponseOrToken();
                // 读到完成的Challenge
                if (serverToken != null) {
                    sendSaslToken(serverToken, false);
                }
                // 身份认证通过，则切换为COMPLETE
                if (saslClient.isComplete()) {
                    setSaslState(SaslState.COMPLETE);
                    transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
                }
                break;
            case COMPLETE:
                break;
            case FAILED:
                throw new IOException("SASL handshake failed");
        }
    }

    private void setSaslState(SaslState saslState) {
        if (netOutBuffer != null && !netOutBuffer.completed())
            pendingSaslState = saslState;
        else {
            this.pendingSaslState = null;
            this.saslState = saslState;
            LOG.debug("Set SASL client state to {}", saslState);
        }
    }
    // 处理服务端发送过来的Challenge信息，并将得到新的response信息发送给服务端
    private void sendSaslToken(byte[] serverToken, boolean isInitial) throws IOException {
        if (!saslClient.isComplete()) {
            // 处理challenge信息
            byte[] saslToken = createSaslToken(serverToken, isInitial);
            if (saslToken != null)
                send(new NetworkSend(node, ByteBuffer.wrap(saslToken)));
        }
    }

    // 负责发送输出缓冲区中的数据
    private void send(Send send) throws IOException {
        try {
            netOutBuffer = send;
            flushNetOutBufferAndUpdateInterestOps();
        } catch (IOException e) {
            setSaslState(SaslState.FAILED);
            throw e;
        }
    }

    private boolean flushNetOutBufferAndUpdateInterestOps() throws IOException {
        // 发送数据
        boolean flushedCompletely = flushNetOutBuffer();
        // 全部发送完 取消对OP_WRITE事件的关注
        if (flushedCompletely) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            if (pendingSaslState != null)
                setSaslState(pendingSaslState);
        } else
            // 未发送完，则继续关注OP_WRITE
            transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        return flushedCompletely;
    }
    // 负责从SocketChannel中读取一个完成的消息
    private byte[] receiveResponseOrToken() throws IOException {
        // 创建缓冲区
        if (netInBuffer == null) netInBuffer = new NetworkReceive(node);
        // 从socketChannel中读取数据
        netInBuffer.readFrom(transportLayer);
        byte[] serverPacket = null;
        // 如果读取到一个完成的消息，则返回读取的负载数据
        if (netInBuffer.complete()) {
            netInBuffer.payload().rewind();
            serverPacket = new byte[netInBuffer.payload().remaining()];
            netInBuffer.payload().get(serverPacket, 0, serverPacket.length);
            netInBuffer = null; // reset the networkReceive as we read all the data.
        }
        return serverPacket;
    }

    public Principal principal() {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, clientPrincipalName);
    }

    public boolean complete() {
        return saslState == SaslState.COMPLETE;
    }

    public void close() throws IOException {
        if (saslClient != null)
            saslClient.dispose();
        if (callbackHandler != null)
            callbackHandler.close();
    }

    private byte[] createSaslToken(final byte[] saslToken, boolean isInitial) throws SaslException {
        if (saslToken == null)
            throw new SaslException("Error authenticating with the Kafka Broker: received a `null` saslToken.");

        try {
            // 初始Response的处理
            if (isInitial && !saslClient.hasInitialResponse())
                return saslToken;
            else
                return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                    public byte[] run() throws SaslException {
                        // 调用evalueteChallenge方法处理Challenge信息
                        return saslClient.evaluateChallenge(saslToken);
                    }
                });
        } catch (PrivilegedActionException e) {
            String error = "An error: (" + e + ") occurred when evaluating SASL token received from the Kafka Broker.";
            // Try to provide hints to use about what went wrong so they can fix their configuration.
            // TODO: introspect about e: look for GSS information.
            final String unknownServerErrorText =
                "(Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)";
            if (e.toString().contains(unknownServerErrorText)) {
                error += " This may be caused by Java's being unable to resolve the Kafka Broker's" +
                    " hostname correctly. You may want to try to adding" +
                    " '-Dsun.net.spi.nameservice.provider.1=dns,sun' to your client's JVMFLAGS environment." +
                    " Users must configure FQDN of kafka brokers when authenticating using SASL and" +
                    " `socketChannel.socket().getInetAddress().getHostName()` must match the hostname in `principal/hostname@realm`";
            }
            error += " Kafka Client will go to AUTH_FAILED state.";
            //Unwrap the SaslException inside `PrivilegedActionException`
            throw new SaslException(error, e.getCause());
        }
    }

    private boolean flushNetOutBuffer() throws IOException {
        if (!netOutBuffer.completed()) {
            // 写入socketChannel中
            netOutBuffer.writeTo(transportLayer);
        }
        return netOutBuffer.completed();
    }

    private void handleKafkaResponse(RequestHeader requestHeader, byte[] responseBytes) {
        AbstractResponse response;
        ApiKeys apiKey;
        try {
            response = NetworkClient.parseResponse(ByteBuffer.wrap(responseBytes), requestHeader);
            apiKey = ApiKeys.forId(requestHeader.apiKey());
        } catch (SchemaException | IllegalArgumentException e) {
            LOG.debug("Invalid SASL mechanism response, server may be expecting only GSSAPI tokens");
            throw new AuthenticationException("Invalid SASL mechanism response", e);
        }
        switch (apiKey) {
            case SASL_HANDSHAKE:
                handleSaslHandshakeResponse((SaslHandshakeResponse) response);
                break;
            default:
                throw new IllegalStateException("Unexpected API key during handshake: " + apiKey);
        }
    }

    private void handleSaslHandshakeResponse(SaslHandshakeResponse response) {
        Errors error = response.error();
        switch (error) {
            case NONE:
                break;
            case UNSUPPORTED_SASL_MECHANISM:
                throw new UnsupportedSaslMechanismException(String.format("Client SASL mechanism '%s' not enabled in the server, enabled mechanisms are %s",
                    mechanism, response.enabledMechanisms()));
            case ILLEGAL_SASL_STATE:
                throw new IllegalSaslStateException(String.format("Unexpected handshake request with client mechanism %s, enabled mechanisms are %s",
                    mechanism, response.enabledMechanisms()));
            default:
                throw new AuthenticationException(String.format("Unknown error code %s, client mechanism is %s, enabled mechanisms are %s",
                    response.error(), mechanism, response.enabledMechanisms()));
        }
    }

    /**
     * Returns the first Principal from Subject.
     * @throws KafkaException if there are no Principals in the Subject.
     *     During Kerberos re-login, principal is reset on Subject. An exception is
     *     thrown so that the connection is retried after any configured backoff.
     */
    static final String firstPrincipal(Subject subject) {
        Set<Principal> principals = subject.getPrincipals();
        synchronized (principals) {
            Iterator<Principal> iterator = principals.iterator();
            if (iterator.hasNext())
                return iterator.next().getName();
            else
                throw new KafkaException("Principal could not be determined from Subject, this may be a transient failure due to Kerberos re-login");
        }
    }
}
