-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Migrate catalog_metastore from 'custom' to 'gravitino'.
-- The catalog_properties JSON is also updated:
--   1. "uri" is rewritten from the Iceberg REST endpoint (e.g.
--      "http://<clb_ip>:9001/iceberg") to the Gravitino server endpoint
--      "http://<clb_ip>:8090". The CLB IP is extracted from the existing
--      uri via a CTE, then used in the UPDATE.
--   2. "table-formats" is set to "ICEBERG,MIXED_ICEBERG,LANCE"
-- All other keys in catalog_properties remain unchanged.
--
-- Uses a WITH (CTE) intermediate table instead of SELECT ... INTO @var,
-- because INTO @var is not supported in some MySQL execution contexts
-- (e.g. JDBC batch, certain drivers, or non-stored-procedure scripts).

WITH clb_ip_cte AS (
    SELECT SUBSTRING_INDEX(
               SUBSTRING_INDEX(
                   SUBSTRING_INDEX(
                       JSON_UNQUOTE(JSON_EXTRACT(`catalog_properties`, '$."uri"')),
                       '://', -1
                   ),
                   '/', 1
               ),
               ':', 1
           ) AS clb_ip
    FROM `catalog_metadata`
    WHERE `catalog_metastore` = 'custom'
    LIMIT 1
)
UPDATE `catalog_metadata` cm
CROSS JOIN clb_ip_cte c
SET cm.`catalog_metastore`  = 'gravitino',
    cm.`catalog_properties` = JSON_SET(
        JSON_SET(
            cm.`catalog_properties`,
            '$."uri"', CONCAT('http://', c.clb_ip, ':8090')
        ),
        '$."table-formats"', 'ICEBERG,MIXED_ICEBERG,LANCE'
    )
WHERE cm.`catalog_metastore` = 'custom';
