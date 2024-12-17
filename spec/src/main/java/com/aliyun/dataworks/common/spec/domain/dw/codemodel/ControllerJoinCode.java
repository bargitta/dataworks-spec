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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;
import com.aliyun.dataworks.common.spec.utils.GsonUtils;
import com.aliyun.dataworks.common.spec.utils.StringTypeObjectAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * 归并节点代码模型
 *
 * @author 聿剑
 * @date 2022/10/28
 */
@Data
@ToString(callSuper = true)
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
public class ControllerJoinCode extends AbstractBaseCode implements JsonFormCode {
    /**
     * 归并条件定义
     */
    @Data
    @ToString
    @Accessors(chain = true)
    @EqualsAndHashCode
    public static class Branch {
        private Integer logic;
        // 上游节点输出
        private String node;
        private String nodeName;
        private String nodeUuid;
        private String projectIdentifier;
        // 运行状态等于
        private List<String> runStatus;
    }

    @Getter
    @AllArgsConstructor
    public enum Status implements LabelEnum {

        /**
         * 运行失败
         */
        FAILURE("Failure", "0"),
        /**
         * 运行成功
         */
        SUCCESS("Success", "1"),
        /**
         * 分支未运行
         */
        NOT_RUN("NotRun", "2");
        private final String label;
        private final String code;
    }

    @Getter
    @AllArgsConstructor
    public enum Logic implements LabelEnum {

        /**
         * 或
         */
        OR("Or", 0),
        /**
         * 且
         */
        AND("And", 1);
        private final String label;
        private final Integer code;

        public static Logic ofCode(Integer code) {
            for (Logic value : Logic.values()) {
                if (value.getCode().equals(code)) {
                    return value;
                }
            }
            return null;
        }
    }

    /**
     * 归并分支和条件设置
     */
    @JsonAdapter(StringTypeObjectAdapter.class)
    private List<Branch> branchList;

    /**
     * 执行结果设置，设置本节点运行状态为
     */
    private String resultStatus;

    @Override
    public ControllerJoinCode parse(String code) {
        Optional.ofNullable(GsonUtils.fromJsonString(code, new TypeToken<ControllerJoinCode>() {}.getType()))
            .map(m -> (ControllerJoinCode)m)
            .ifPresent(m -> {
                this.setResourceReferences(m.getResourceReferences());
                this.setBranchList(m.getBranchList());
                this.setResultStatus(m.getResultStatus());
            });
        return this;
    }

    @Override
    public Map<String, Object> getTemplate() {
        return null;
    }

    @Override
    public List<String> getProgramTypes() {
        return Collections.singletonList(CodeProgramType.CONTROLLER_JOIN.name());
    }
}
