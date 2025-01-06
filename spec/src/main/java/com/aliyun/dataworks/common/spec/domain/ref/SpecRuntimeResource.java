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

package com.aliyun.dataworks.common.spec.domain.ref;

import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author yiwei.qyw
 * @date 2023/7/4
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SpecRuntimeResource extends SpecRefEntity {
    private String resourceGroup;
    private String resourceGroupId;
    /**
     * 运行时资源，指定cu数
     *
     * @see SpecScriptRuntime#cu
     * @deprecated
     */
    @Deprecated
    private String cu;
}