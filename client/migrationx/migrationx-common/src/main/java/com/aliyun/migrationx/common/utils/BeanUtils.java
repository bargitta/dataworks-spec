/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.migrationx.common.utils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.beanutils.PropertyUtils;
import org.dozer.DozerBeanMapper;
/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-06-06
 */
public class BeanUtils {

    private static final DozerBeanMapper DOZER_BEAN_MAPPER = new DozerBeanMapper();

    public static <T> T deepCopy(Object obj, Class<T> clazz) {
        if (obj == null || clazz == null) {
            return null;
        }
        if (!clazz.isInstance(obj)) {
            throw new IllegalArgumentException("obj is not instance of clazz");
        }
        return DOZER_BEAN_MAPPER.map(obj, clazz);
    }

    private BeanUtils() {

    }

    public static void copyProperties(Object src, Object dest) {
        try {
            org.apache.commons.beanutils.BeanUtils.copyProperties(dest, src);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void copyProperties(Object src, Object dest, String... ignoreProperties) {
        try {
            copyPropertiesWithIgnore(dest, src, ignoreProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void copyPropertiesWithIgnore(Object dest, Object orig, String... ignoreProperties)
            throws IllegalAccessException, InvocationTargetException {
        List<String> ignoreList = (ignoreProperties != null) ? Arrays.asList(ignoreProperties) : null;

        PropertyDescriptor[] origDescriptors = PropertyUtils.getPropertyDescriptors(orig);
        for (PropertyDescriptor origDescriptor : origDescriptors) {
            String name = origDescriptor.getName();
            if ("class".equals(name)) {
                continue; // Ignore the "class" property
            }
            if (ignoreList != null && ignoreList.contains(name)) {
                continue; // Ignore specified properties
            }

            if (PropertyUtils.isReadable(orig, name) && PropertyUtils.isWriteable(dest, name)) {
                try {
                    Object value = PropertyUtils.getSimpleProperty(orig, name);
                    org.apache.commons.beanutils.BeanUtils.setProperty(dest, name, value);
                } catch (NoSuchMethodException e) {
                    // Ignore if the setter method does not exist in the destination bean
                }
            }
        }
    }
}