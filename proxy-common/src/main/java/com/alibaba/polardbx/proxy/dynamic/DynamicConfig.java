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

package com.alibaba.polardbx.proxy.dynamic;

import com.alibaba.polardbx.proxy.common.XClusterNodeBasic;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

@Getter
@Setter
public class DynamicConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConfig.class);
    public static final Gson GSON = new GsonBuilder().create();

    @SerializedName("XCluster")
    private List<XClusterNodeBasic> XCluster;

    private DynamicConfig() {
    }

    private static DynamicConfig nowConfig;

    private static boolean recover(final String jsonFile) {
        final File file = new File(jsonFile);
        if (file.isDirectory()) {
            throw new IllegalArgumentException("jsonFile must be a file");
        } else if (!file.exists()) {
            final File bak = new File(jsonFile + ".bak");
            if (!bak.isDirectory() && bak.exists()) {
                // try restore from backup
                return bak.renameTo(file);
            }
        }
        return false;
    }

    public static synchronized DynamicConfig reload() {
        final String jsonFile = ConfigLoader.PROPERTIES.getProperty(ConfigProps.DYNAMIC_CONFIG_FILE);
        try (final BufferedReader reader = new BufferedReader(new FileReader(jsonFile))) {
            nowConfig = GSON.fromJson(reader, DynamicConfig.class);
            if (null == nowConfig) {
                throw new IllegalArgumentException("dynamic config must be not null");
            }
            return nowConfig;
        } catch (Exception e0) {
            if (!(e0 instanceof FileNotFoundException)) {
                LOGGER.error("error when load original dynamic file", e0);
                // bad file, delete it and try recover
                final boolean ignored = new File(jsonFile).delete();
            }
            // try recover
            if (recover(jsonFile)) {
                try (final BufferedReader reader = new BufferedReader(new FileReader(jsonFile))) {
                    nowConfig = GSON.fromJson(reader, DynamicConfig.class);
                    if (null == nowConfig) {
                        throw new IllegalArgumentException("dynamic config must be not null");
                    }
                    return nowConfig;
                } catch (Exception e1) {
                    LOGGER.error("error when recover and read dynamic file", e1);
                    // bad file in bak, delete it and return empty config
                    final boolean ignored = new File(jsonFile).delete();
                    nowConfig = new DynamicConfig();
                }
            } else {
                // no dynamic config
                nowConfig = new DynamicConfig();
            }
        }
        return nowConfig;
    }

    public static synchronized DynamicConfig getNowConfig() {
        if (null == nowConfig) {
            return reload();
        }
        return nowConfig;
    }

    public synchronized void save() throws IOException {
        synchronized (DynamicConfig.class) {
            final String jsonFile = ConfigLoader.PROPERTIES.getProperty(ConfigProps.DYNAMIC_CONFIG_FILE);
            // move to bak first if exist
            final File file = new File(jsonFile);
            if (!file.isDirectory() && file.exists()) {
                final File bak = new File(jsonFile + ".bak");
                if (!bak.isDirectory() && bak.exists()) {
                    final boolean ignored = bak.delete();
                }
                final boolean ignored = file.renameTo(bak);
            }
            try (final Writer writer = new FileWriter(jsonFile)) {
                GSON.toJson(this, writer);
            }
        }
    }
}
