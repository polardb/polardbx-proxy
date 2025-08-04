/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.proxy;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GeneralService extends GeneralServiceGrpc.GeneralServiceImplBase {
    private static final String OK_RESULT = "ok";
    private static final String NO_HANDLER_RESULT = "no_handler";
    private static final Map<String, ServiceHandler> HANDLER_MAP = new ConcurrentHashMap<>();

    public static ServiceHandler registerHandler(String name, ServiceHandler handler) {
        return HANDLER_MAP.put(name, handler);
    }

    public static ServiceHandler unregisterHandler(String name) {
        return HANDLER_MAP.remove(name);
    }

    @Override
    public void generalRemoteProcedure(GeneralServiceProto.GeneralRequest request,
                                       StreamObserver<GeneralServiceProto.GeneralResponse> responseObserver) {
        final String type = request.getType();
        final ServiceHandler handler = HANDLER_MAP.get(type);

        final GeneralServiceProto.GeneralResponse response;
        if (null == handler) {
            response = GeneralServiceProto.GeneralResponse.newBuilder()
                .setResult(NO_HANDLER_RESULT)
                .build();
        } else {
            final String json = handler.handle(request.getJson());
            response = GeneralServiceProto.GeneralResponse.newBuilder()
                .setResult(OK_RESULT)
                .setJson(json)
                .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void startServer(int port) throws IOException {
        ServerBuilder.forPort(port)
            .addService(new GeneralService())
            .build()
            .start();
    }

    public static String invoke(final String host, final int port, final String type, final String json,
                                final long timeoutMillis) {
        final ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();

        try {
            final GeneralServiceGrpc.GeneralServiceBlockingStub stub = GeneralServiceGrpc.newBlockingStub(channel);
            final GeneralServiceProto.GeneralRequest request = GeneralServiceProto.GeneralRequest.newBuilder()
                .setType(type)
                .setJson(json)
                .build();
            final GeneralServiceProto.GeneralResponse response =
                stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).generalRemoteProcedure(request);
            return response.getResult().equals(OK_RESULT) ? response.getJson() : null;
        } finally {
            channel.shutdownNow();
        }
    }
}
