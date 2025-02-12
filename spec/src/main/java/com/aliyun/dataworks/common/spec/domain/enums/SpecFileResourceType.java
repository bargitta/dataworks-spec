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

package com.aliyun.dataworks.common.spec.domain.enums;

import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;

import lombok.extern.slf4j.Slf4j;

/**
 * File resource types
 *
 * @author 聿剑
 * @date 2023/11/29
 */
@Slf4j
public enum SpecFileResourceType implements LabelEnum {
    /**
     * Python file resource type
     */
    PYTHON("python"),

    /**
     * Java jar file resource type
     */
    JAR("jar"),

    /**
     * Archive file resource type
     */
    ARCHIVE("archive"),

    /**
     * Text file resource type
     */
    FILE("file"),
    ;

    private final String label;

    SpecFileResourceType(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

    public SpecFileResourceType of(String label) {
        if (label == null) {
            log.error("label is null");
        }
        for (SpecFileResourceType type : SpecFileResourceType.values()) {
            if (type.label.equalsIgnoreCase(label)) {
                return type;
            }
        }
        log.error("SpecFileResourceType {} not found", label);
        return null;
    }
}
