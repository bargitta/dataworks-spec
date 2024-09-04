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

package com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums;

/**
 * http check condition
 */
public enum HttpCheckCondition {
    /**
     * 0 status_code_default:200
     * 1 status_code_custom
     * 2 body_contains
     * 3 body_not_contains
     */
    STATUS_CODE_DEFAULT,
    STATUS_CODE_CUSTOM,
    BODY_CONTAINS,
    BODY_NOT_CONTAINS
}