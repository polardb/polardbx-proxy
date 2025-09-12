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

package com.alibaba.polardbx.proxy.privilege;

import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

public class SecurityUtil {
    public static final ThreadLocal<MessageDigest> SHA1 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    });
    public static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    });

    /**
     * 获取数据库保存的密码mysql.user.Password
     *
     * @param plainPassword 明文密码
     */
    public static byte[] calcMysqlUserPassword(byte[] plainPassword) {
        final MessageDigest md = SHA1.get();
        md.reset();
        final byte[] pass1 = md.digest(plainPassword);
        md.reset();
        return md.digest(pass1);
    }

    /**
     * mysql服务端认证密码算法
     *
     * @param token 经过客户端加密算法算出的密码
     * @param mysqlUserPassword 数据库保存的密码
     * @param scramble 服务器发送给客户端的随机字符串
     */
    public static boolean verify(byte[] token, byte[] mysqlUserPassword, byte[] scramble, int offset, int length) {
        final MessageDigest md = SHA1.get();

        md.reset();
        md.update(scramble, offset, length);
        final byte[] stage1_hash = md.digest(mysqlUserPassword);
        for (int i = 0; i < stage1_hash.length; i++) {
            stage1_hash[i] = (byte) (stage1_hash[i] ^ token[i]);
        }

        md.reset();
        final byte[] candidate_hash2 = md.digest(stage1_hash);
        return Arrays.equals(mysqlUserPassword, candidate_hash2);
    }

    public static byte[] challenge(byte[] plainPassword, byte[] scramble, int offset, int length) {
        final MessageDigest md = SHA1.get();
        md.reset();
        final byte[] stage1_hash = md.digest(plainPassword);
        md.reset();
        final byte[] stage2_hash = md.digest(stage1_hash);
        md.reset();
        md.update(scramble, offset, length);
        final byte[] token = md.digest(stage2_hash);
        for (int i = 0; i < stage1_hash.length; i++) {
            token[i] = (byte) (token[i] ^ stage1_hash[i]);
        }
        return token;
    }

    private static final int CACHING_SHA2_DIGEST_LENGTH = 32;

    public static byte[] challengeCachingSha2(
        byte[] plainPassword, byte[] seed, int offset, int length) throws DigestException {
        final MessageDigest md = SHA256.get();
        md.reset();

        final byte[] dig1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        final byte[] dig2 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        final byte[] scramble1 = new byte[CACHING_SHA2_DIGEST_LENGTH];

        // SHA2(src) => digest_stage1
        md.update(plainPassword, 0, plainPassword.length);
        md.digest(dig1, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        // SHA2(digest_stage1) => digest_stage2
        md.update(dig1, 0, dig1.length);
        md.digest(dig2, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        // SHA2(digest_stage2, m_rnd) => scramble_stage1
        md.update(dig2, 0, dig1.length);
        md.update(seed, offset, length);
        md.digest(scramble1, 0, CACHING_SHA2_DIGEST_LENGTH);

        // XOR(digest_stage1, scramble_stage1) => scramble
        byte[] mysqlScrambleBuff = new byte[CACHING_SHA2_DIGEST_LENGTH];
        xorString(dig1, mysqlScrambleBuff, scramble1, CACHING_SHA2_DIGEST_LENGTH);

        return mysqlScrambleBuff;
    }

    private static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
        int pos = 0;
        int scrambleLength = scramble.length;
        while (pos < length) {
            to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
            pos++;
        }
    }

    private static final String DN_PASSWORD_KEY = "dnPasswordKey";

    public static String encrypt(String sSrc) {
        return encrypt(sSrc, null);
    }

    public static String decrypt(String sSrc) {
        return decrypt(sSrc, null);
    }

    // Encode, AES + Base64
    public static String encrypt(String sSrc, String key) {
        String sKey = key;
        // Overwrite key with environment variable.
        final String configKey = ConfigLoader.PROPERTIES.getProperty(ConfigProps.DN_PASSWORD_KEY);
        if (null != System.getenv(DN_PASSWORD_KEY)) {
            sKey = System.getenv(DN_PASSWORD_KEY);
        } else if (configKey != null && !configKey.isEmpty()) {
            sKey = configKey;
        }
        return null == sKey ? sSrc : encryptByKey(sSrc, sKey);
    }

    // Decode, AES + Base64
    public static String decrypt(String sSrc, String key) {
        String sKey = key;
        // Overwrite key with environment variable.
        final String configKey = ConfigLoader.PROPERTIES.getProperty(ConfigProps.DN_PASSWORD_KEY);
        if (null != System.getenv(DN_PASSWORD_KEY)) {
            sKey = System.getenv(DN_PASSWORD_KEY);
        } else if (configKey != null && !configKey.isEmpty()) {
            sKey = configKey;
        }
        return null == sKey ? sSrc : decryptByKey(sSrc, sKey);
    }

    public static String encryptByKey(String sSrc, String key) {
        if (key.isEmpty()) {
            return sSrc;
        }
        try {
            final byte[] raw = key.getBytes(StandardCharsets.UTF_8);
            final SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            // "算法/模式/补码方式"
            final Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            final byte[] encrypted = cipher.doFinal(sSrc.getBytes(StandardCharsets.UTF_8));
            // 此处使用BASE64做转码功能，同时能起到2次加密的作用。
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Throwable ex) {
            throw new RuntimeException("param error during encrypt", ex);
        }
    }

    // Decode, AES + Base64
    public static String decryptByKey(String sSrc, String key) {
        if (key.isEmpty()) {
            return sSrc;
        }
        try {
            final byte[] raw = key.getBytes(StandardCharsets.UTF_8);
            final SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            final Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            // 先用base64解密
            final byte[] encrypted1 = Base64.getDecoder().decode(sSrc);
            final byte[] original = cipher.doFinal(encrypted1);
            return new String(original, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new RuntimeException("param error during decrypt", ex);
        }
    }

    public static void main(String[] args) {
        // 2 args required
        if (args.length != 2) {
            System.out.println("Usage: java SecurityUtil <plainPassword> <key>");
            return;
        }
        // check key size
        if (args[1].length() != 16) {
            System.out.println("Key size must be 16");
            return;
        }
        // check key is hex
        for (int i = 0; i < args[1].length(); i++) {
            if (!Character.isDigit(args[1].charAt(i)) && !Character.isLetter(args[1].charAt(i))) {
                System.out.println("Key must be hex");
                return;
            }
        }
        // output encrypted password
        System.out.println(encryptByKey(args[0], args[1]));
    }
}
