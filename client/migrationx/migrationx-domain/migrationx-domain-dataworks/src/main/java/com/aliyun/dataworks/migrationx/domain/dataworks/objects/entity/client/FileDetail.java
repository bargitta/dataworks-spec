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

package com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client;

import java.util.List;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * @author sam.liux
 * @date 2020/04/30
 */
@Data
@ToString(callSuper = true)
@Accessors(chain = true)
public class FileDetail {
    private File file;
    private FileNodeCfg nodeCfg;
    private Boolean isRefUpdate;
    private List<FileNode> nodes;
    private List<FileRelation> relations;
    private String sourceApp;
    private DiFileDsExt diFileDsExt;
    private ResourceDownloadInfo resourceDownloadLinkDto;
}
