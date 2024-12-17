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

package com.aliyun.dataworks.common.spec.utils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author 聿剑
 * @date 2024/11/21
 */
public class ObjectUtils {
    public static void copyProperties(Object source, Object target) {
        if (source == null || target == null) {
            return;
        }

        for (java.lang.reflect.Field field : ReflectUtils.getPropertyFields(source)) {
            field.setAccessible(true);
            List<Field> targetFields = ReflectUtils.getPropertyFields(target);
            for (Field targetField : targetFields) {
                if (!targetField.getName().equals(field.getName())) {
                    continue;
                }

                try {
                    targetField.setAccessible(true);
                    targetField.set(target, field.get(source));
                } catch (IllegalAccessException e) {
                    // ignore
                }
            }
        }
    }
}
