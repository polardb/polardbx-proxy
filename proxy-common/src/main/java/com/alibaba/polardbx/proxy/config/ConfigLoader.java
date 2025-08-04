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

package com.alibaba.polardbx.proxy.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class ConfigLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigLoader.class);

    private static final String CONFIG_KEY = "server.conf";
    private static final String CONFIG_FILE = "config.properties";
    private static final String SERVER_ARGS = "serverArgs";

    public static final Properties PROPERTIES = new Properties(ConfigProps.DEFAULT_PROPS);

    public static void loadConfig() throws IOException {
        // try load from file or resources
        final String conf = System.getProperty(CONFIG_KEY);
        try (final InputStream in = conf != null ? new FileInputStream(conf) :
            ConfigLoader.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (in != null) {
                PROPERTIES.load(in);
            }
        }

        // renew with server properties and env
        PROPERTIES.putAll(System.getProperties());
        PROPERTIES.putAll(System.getenv());

        // renew with server args
        final String serverArgs = System.getProperty(SERVER_ARGS);
        if (serverArgs != null && !serverArgs.isEmpty()) {
            final String[] args = serverArgs.split(";");
            for (String arg : args) {
                final String[] config = arg.split("=");
                if (config.length != 2) {
                    throw new RuntimeException("config is error: " + Arrays.toString(config));
                }
                PROPERTIES.put(config[0].trim(), config[1].trim());
            }
        }

        // remove all unknown config
        PROPERTIES.keySet().removeIf(key -> !ConfigProps.DEFAULT_PROPS.containsKey(key));

        // print it
        LOGGER.info("server config: {}", PROPERTIES);
    }
}
