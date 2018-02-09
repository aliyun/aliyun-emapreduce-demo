/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.example.spark;

import org.apache.hadoop.conf.Configuration;

public class EMapReduceOSSUtil {

    private static String SCHEMA = "oss://";
    private static String AKSEP = ":";
    private static String BKTSEP = "@";
    private static String EPSEP = ".";
    private static String HTTP_HEADER = "http://";

    /**
     * complete OSS uri
     * convert uri like: oss://bucket/path  to  oss://accessKeyId:accessKeySecret@bucket.endpoint/path
     * ossref do not need this
     *
     * @param oriUri original OSS uri
     */
    public static String buildOSSCompleteUri(String oriUri, String akId, String akSecret, String endpoint) {
        if (akId == null) {
            System.err.println("miss accessKeyId");
            return oriUri;
        }
        if (akSecret == null) {
            System.err.println("miss accessKeySecret");
            return oriUri;
        }
        if (endpoint == null) {
            System.err.println("miss endpoint");
            return oriUri;
        }

        int index = oriUri.indexOf(SCHEMA);
        if (index == -1 || index != 0) {
            return oriUri;
        }

        int bucketIndex = index + SCHEMA.length();
        int pathIndex = oriUri.indexOf("/", bucketIndex);
        String bucket = null;
        if (pathIndex == -1) {
            bucket = oriUri.substring(bucketIndex);
        } else {
            bucket = oriUri.substring(bucketIndex, pathIndex);
        }

        StringBuilder retUri = new StringBuilder();
        retUri.append(SCHEMA)
                .append(akId)
                .append(AKSEP)
                .append(akSecret)
                .append(BKTSEP)
                .append(bucket)
                .append(EPSEP)
                .append(stripHttp(endpoint));

        if (pathIndex > 0) {
            retUri.append(oriUri.substring(pathIndex));
        }

        return retUri.toString();
    }

    public static String buildOSSCompleteUri(String oriUri, Configuration conf) {
        return buildOSSCompleteUri(oriUri, conf.get("fs.oss.accessKeyId"), conf.get("fs.oss.accessKeySecret"), conf.get("fs.oss.endpoint"));
    }

    private static String stripHttp(String endpoint) {
        if (endpoint.startsWith(HTTP_HEADER)) {
            return endpoint.substring(HTTP_HEADER.length());
        }
        return endpoint;
    }
}
