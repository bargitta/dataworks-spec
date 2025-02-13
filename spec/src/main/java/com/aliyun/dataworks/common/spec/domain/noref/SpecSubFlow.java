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

package com.aliyun.dataworks.common.spec.domain.noref;

import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 子Workflow Spec对象定义
 *
 * @author sam.liux
 * @date 2023/10/25
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SpecSubFlow extends SpecWorkflow {
    /**
     * 通过output引用的Flow
     */
    private String output;
}