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

package com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @author sam.liux
 * @date 2020/02/13
 */
@ToString(callSuper = true, exclude = {"projectRef"})
@Slf4j
public class DwDatasource extends Datasource {
    @JsonIgnore
    private transient Project projectRef;

    @Override
    public String getUniqueKey() {
        if (StringUtils.isEmpty(getName()) || StringUtils.isEmpty(getType())) {
            log.warn("getUniqueKey is empty, name:{},type:{}", getName(), getType());
            return UUID.randomUUID().toString();
        }
        String str = Joiner.on("#").join(
                getName(), getType(),
                StringUtils.defaultIfBlank(getEnvType(), ""),
                StringUtils.defaultIfBlank(getSubType(), ""));
        return UUID.nameUUIDFromBytes(str.getBytes()).toString();
    }

    public Project getProjectRef() {
        return projectRef;
    }

    public void setProjectRef(Project projectRef) {
        this.projectRef = projectRef;
    }
}
