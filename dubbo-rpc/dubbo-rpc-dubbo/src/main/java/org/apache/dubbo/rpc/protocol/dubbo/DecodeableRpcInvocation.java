/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.dubbo;


import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Cleanable;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Decodeable;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.dubbo.common.URL.buildKey;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.remoting.Constants.DEFAULT_REMOTING_SERIALIZATION;
import static org.apache.dubbo.remoting.Constants.SERIALIZATION_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.CallbackServiceCodec.decodeInvocationArgument;

public class DecodeableRpcInvocation extends RpcInvocation implements Codec, Decodeable {

    private static final Logger log = LoggerFactory.getLogger(DecodeableRpcInvocation.class);

    private Channel channel;

    private byte serializationType;

    private InputStream inputStream;

    private Request request;

    private volatile boolean hasDecoded;

    public DecodeableRpcInvocation(Channel channel, Request request, InputStream is, byte id) {
        Assert.notNull(channel, "channel == null");
        Assert.notNull(request, "request == null");
        Assert.notNull(is, "inputStream == null");
        this.channel = channel;
        this.request = request;
        this.inputStream = is;
        this.serializationType = id;
    }

    @Override
    public void decode() throws Exception {
        if (!hasDecoded && channel != null && inputStream != null) {
            try {
                decode(channel, inputStream);
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode rpc invocation failed: " + e.getMessage(), e);
                }
                request.setBroken(true);
                request.setData(e);
            } finally {
                hasDecoded = true;
            }
        }
    }

    @Override
    public void encode(Channel channel, OutputStream output, Object message) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object decode(Channel channel, InputStream input) throws IOException {
        ObjectInput in = CodecSupport.getSerialization(channel.getUrl(), serializationType)
                .deserialize(channel.getUrl(), input);

        String dubboVersion = in.readUTF();
        request.setVersion(dubboVersion);
        setAttachment(DUBBO_VERSION_KEY, dubboVersion);

        String path = in.readUTF();
        setAttachment(PATH_KEY, path);
        setAttachment(VERSION_KEY, in.readUTF());
        setMethodName(in.readUTF());

        try {
            Object[] args = DubboCodec.EMPTY_OBJECT_ARRAY;
            Class<?>[] pts = DubboCodec.EMPTY_CLASS_ARRAY;

            //modify by chaihaipeng
            Object[] result = null;
            if ("2.8.4-SNAPSHOT".equals(dubboVersion)) {
                result = decodeFor284Snapshot(in);
            } else if ("2.8.4".equals(dubboVersion)) {
                result = decodeFor284(in);
            } else {
                result = decodeFor276(in, path);
            }
            if (result != null && result.length == 2) {
                args = (Object[]) result[0];
                pts = (Class<?>[]) result[1];
            }

            setParameterTypes(pts);

            Map<String, Object> map = in.readAttachments();
            if (map != null && map.size() > 0) {
                Map<String, Object> attachment = getObjectAttachments();
                if (attachment == null) {
                    attachment = new HashMap<>();
                }
                attachment.putAll(map);
                setObjectAttachments(attachment);
            }

            //decode argument ,may be callback
            for (int i = 0; i < args.length; i++) {
                args[i] = decodeInvocationArgument(channel, this, pts, i, args[i]);
            }

            setArguments(args);
            String targetServiceName = buildKey((String) getAttachment(PATH_KEY),
                    getAttachment(GROUP_KEY),
                    getAttachment(VERSION_KEY));
            setTargetServiceUniqueName(targetServiceName);
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read invocation data failed.", e));
        } finally {
            if (in instanceof Cleanable) {
                ((Cleanable) in).cleanup();
            }
        }
        return this;
    }

    private Object[] decodeFor276(ObjectInput in, String path) throws IOException, ClassNotFoundException {
        Object[] args = DubboCodec.EMPTY_OBJECT_ARRAY;
        Class<?>[] pts = DubboCodec.EMPTY_CLASS_ARRAY;

        String desc = in.readUTF();
        setParameterTypesDesc(desc);

        if (desc.length() > 0) {
            ServiceRepository repository = ApplicationModel.getServiceRepository();
            ServiceDescriptor serviceDescriptor = repository.lookupService(path);
            if (serviceDescriptor != null) {
                MethodDescriptor methodDescriptor = serviceDescriptor.getMethod(getMethodName(), desc);
                if (methodDescriptor != null) {
                    pts = methodDescriptor.getParameterClasses();
                    this.setReturnTypes(methodDescriptor.getReturnTypes());
                }
            }
            if (pts == DubboCodec.EMPTY_CLASS_ARRAY) {
                pts = ReflectUtils.desc2classArray(desc);
            }
            args = new Object[pts.length];
            for (int i = 0; i < args.length; i++) {
                try {
                    args[i] = in.readObject(pts[i]);
                } catch (Exception e) {
                    if (log.isWarnEnabled()) {
                        log.warn("Decode argument failed: " + e.getMessage(), e);
                    }
                }
            }
        }
        Object[] result = new Object[2];
        result[0] = args;
        result[1] = pts;
        return result;
    }

    private Object[] decodeFor284(ObjectInput in) throws IOException, ClassNotFoundException {
        Object[] args = DubboCodec.EMPTY_OBJECT_ARRAY;
        Class<?>[] pts = DubboCodec.EMPTY_CLASS_ARRAY;
        int argNum = -1;

        if (optimizedSerializationType(channel.getUrl())) {//kryo or fst
            argNum = in.readInt();
        }
        if (argNum > 0) {
            args = new Object[argNum];
            pts = new Class[argNum];
            for (int i = 0; i < args.length; ++i) {
                try {
                    args[i] = in.readObject();
                    pts[i] = args[i].getClass();
                } catch (Exception var16) {
                    if (log.isWarnEnabled()) {
                        log.warn("Decode for 2.8.4 argument failed: " + var16.getMessage(), var16);
                    }
                }
            }
        } else {
            String desc = in.readUTF();
            if (desc.length() > 0) {
                pts = ReflectUtils.desc2classArray(desc);
                args = new Object[pts.length];

                for (int i = 0; i < args.length; ++i) {
                    try {
                        args[i] = in.readObject(pts[i]);
                    } catch (Exception var15) {
                        if (log.isWarnEnabled()) {
                            log.warn("Decode for 2.8.4 argument failed: " + var15.getMessage(), var15);
                        }
                    }
                }
            }
        }
        Object[] result = new Object[2];
        result[0] = args;
        result[1] = pts;
        return result;
    }

    private boolean optimizedSerializationType(URL url) {
        String serialization = url.getParameter(SERIALIZATION_KEY, DEFAULT_REMOTING_SERIALIZATION);
        return "fst".equalsIgnoreCase(serialization) || "kryo".equalsIgnoreCase(serialization);
    }


    private Object[] decodeFor284Snapshot(ObjectInput in) throws IOException, ClassNotFoundException {
        Object[] args = DubboCodec.EMPTY_OBJECT_ARRAY;
        Class<?>[] pts = DubboCodec.EMPTY_CLASS_ARRAY;

        int argNum = in.readInt();
        if (argNum > 0) {
            args = new Object[argNum];
            pts = new Class[argNum];
            for (int i = 0; i < args.length; ++i) {
                try {
                    args[i] = in.readObject();
                    pts[i] = args[i].getClass();
                } catch (Exception var16) {
                    if (log.isWarnEnabled()) {
                        log.warn("Decode for 2.8.4-SNAPSHOT argument failed: " + var16.getMessage(), var16);
                    }
                }
            }
        } else {
            String desc = in.readUTF();
            if (desc.length() > 0) {
                pts = ReflectUtils.desc2classArray(desc);
                args = new Object[pts.length];

                for (int i = 0; i < args.length; ++i) {
                    try {
                        args[i] = in.readObject(pts[i]);
                    } catch (Exception var15) {
                        if (log.isWarnEnabled()) {
                            log.warn("Decode for 2.8.4-SNAPSHOT argument failed: " + var15.getMessage(), var15);
                        }
                    }
                }
            }
        }
        Object[] result = new Object[2];
        result[0] = args;
        result[1] = pts;
        return result;
    }

}
