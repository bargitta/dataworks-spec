/*
 * Copyright (c) 2024, Alibaba Cloud;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dataworks.common.spec.domain.dw.codemodel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.aliyun.dataworks.common.spec.utils.GsonUtils;
import com.google.common.reflect.TypeToken;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * @author sam.liux
 * @date 2020/04/03
 */
@Data
@Accessors(chain = true)
@ToString
public class EmrAllocationSpec {
    public static final String UPPER_KEY_USE_GATEWAY = "USE_GATEWAY";
    public static final String UPPER_KEY_REUSE_SESSION = "REUSE_SESSION";
    public static final String UPPER_KEY_DATAWORKS_SESSION_DISABLE = "DATAWORKS_SESSION_DISABLE";
    public static final String UPPER_KEY_FLOW_SKIP_SQL_ANALYZE = "FLOW_SKIP_SQL_ANALYZE";
    public static final String UPPER_KEY_ENABLE_SPARKSQL_JDBC = "ENABLE_SPARKSQL_JDBC";
    public static final Set<String> UPPER_KEYS = new HashSet<>(Arrays.asList(
        UPPER_KEY_USE_GATEWAY, UPPER_KEY_REUSE_SESSION, UPPER_KEY_DATAWORKS_SESSION_DISABLE,
        UPPER_KEY_ENABLE_SPARKSQL_JDBC, UPPER_KEY_FLOW_SKIP_SQL_ANALYZE));

    private String queue;
    private String vcores;
    private String memory;
    private String priority;
    private String userName;
    @SerializedName(UPPER_KEY_USE_GATEWAY)
    private Boolean useGateway;
    @SerializedName(UPPER_KEY_REUSE_SESSION)
    private Boolean reuseSession;
    @SerializedName(UPPER_KEY_DATAWORKS_SESSION_DISABLE)
    private Boolean dataworksSessionDisable;
    @SerializedName(UPPER_KEY_FLOW_SKIP_SQL_ANALYZE)
    private Boolean batchMode;
    @SerializedName(UPPER_KEY_ENABLE_SPARKSQL_JDBC)
    private Boolean enableJdbcSql;

    public static EmrAllocationSpec of(Map<String, Object> allocateSpec) {
        if (allocateSpec == null) {
            return null;
        }

        return GsonUtils.fromJsonString(GsonUtils.toJsonString(allocateSpec), EmrAllocationSpec.class);
    }

    public Map<String, Object> toMap() {
        return GsonUtils.fromJsonString(GsonUtils.toJsonString(this), new TypeToken<Map<String, Object>>() {}.getType());
    }
}
