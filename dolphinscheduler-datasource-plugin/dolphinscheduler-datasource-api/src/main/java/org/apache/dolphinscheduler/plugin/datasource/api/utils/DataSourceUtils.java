/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.datasource.api.utils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceProcessorProvider;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.sql.Connection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceUtils {

    public DataSourceUtils() {
    }

    private static final Logger logger = LoggerFactory.getLogger(DataSourceUtils.class);

    /**
     * check datasource param
     *
     * @param baseDataSourceParamDTO datasource param
     */
    public static void checkDatasourceParam(BaseDataSourceParamDTO baseDataSourceParamDTO) {
        getDatasourceProcessor(baseDataSourceParamDTO.getType()).checkDatasourceParam(baseDataSourceParamDTO);
    }

    /**
     * build connection url
     *
     * @param baseDataSourceParamDTO datasourceParam
     */
    public static ConnectionParam buildConnectionParams(BaseDataSourceParamDTO baseDataSourceParamDTO) {
        // 构建连接参数
        ConnectionParam connectionParams = getDatasourceProcessor(baseDataSourceParamDTO.getType())
                .createConnectionParams(baseDataSourceParamDTO);
        logger.info("parameters map:{}", connectionParams);
        return connectionParams;
    }

    public static ConnectionParam buildConnectionParams(DbType dbType, String connectionJson) {
        return getDatasourceProcessor(dbType).createConnectionParams(connectionJson);
    }

    public static String getJdbcUrl(DbType dbType, ConnectionParam baseConnectionParam) {
        return getDatasourceProcessor(dbType).getJdbcUrl(baseConnectionParam);
    }

    /**
     * 获取对应数据源的连接
     * @param dbType
     * @param connectionParam
     * @return
     */
    public static Connection getConnection(DbType dbType, ConnectionParam connectionParam) {
        try {
            return getDatasourceProcessor(dbType).getConnection(connectionParam);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDatasourceDriver(DbType dbType) {
        return getDatasourceProcessor(dbType).getDatasourceDriver();
    }

    public static BaseDataSourceParamDTO buildDatasourceParamDTO(DbType dbType, String connectionParams) {
        return getDatasourceProcessor(dbType).createDatasourceParamDTO(connectionParams);
    }

    public static DataSourceProcessor getDatasourceProcessor(DbType dbType) {
        // 获取所有数据源组成的map
        Map<String, DataSourceProcessor> dataSourceProcessorMap = DataSourceProcessorProvider.getInstance().getDataSourceProcessorMap();
        // 如果没有找到对应的数据库类型，抛出illegal datasource type异常
        if (!dataSourceProcessorMap.containsKey(dbType.name())) {
            throw new IllegalArgumentException("illegal datasource type");
        }
        // 返回对应的数据源处理器
        return dataSourceProcessorMap.get(dbType.name());
    }

    /**
     * get datasource UniqueId
     */
    public static String getDatasourceUniqueId(ConnectionParam connectionParam, DbType dbType) {
        return getDatasourceProcessor(dbType).getDatasourceUniqueId(connectionParam, dbType);
    }

    /**
     * build connection url
     */
    public static BaseDataSourceParamDTO buildDatasourceParam(String param) {
        // 解析param为jsonNode
        JsonNode jsonNodes = JSONUtils.parseObject(param);

        /**
         * 首先获取连接数据库的类型 DbType.ofName(jsonNodes.get("type").asText().toUpperCase())
         * getDataSourceProcessor获取对应的处理器
         * caseDatasourceParamDTO创建对应的java bean
         */
        return getDatasourceProcessor(DbType.ofName(jsonNodes.get("type").asText().toUpperCase()))
                .castDatasourceParamDTO(param);
    }
}
