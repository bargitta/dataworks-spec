from pyspark.sql import SparkSession
from da_utils import *
import sys

if __name__ == '__main__':
    p_date = sys.argv[1]
    P_DATE = date_upper(p_date)
    # spark = SparkSession.builder.getOrCreate()
    spark = SparkSession.builder.config('spark.sql.shuffle.partitions', '2000').getOrCreate()
    
    
    
    sql = f"""

WITH base_data AS (
    SELECT
        message_id,
        BIGINT(device_id)                                                   AS device_id,
        BIGINT(npc_id)                                                      AS npc_id,
        event_name,
        event_page,
        page,
        GET_JSON_OBJECT(properties,'$.chat_id')                             AS chat_id,
        -- CASE 
        --     WHEN UPPER(properties_lib) = 'ANDROID' THEN IF(GET_JSON_OBJECT(properties,'$.rec_type') IS NOT NULL, 'rec_reply', 'normal')
        --     WHEN UPPER(properties_lib) = 'IOS'  THEN COALESCE(GET_JSON_OBJECT(properties,'$.rec_type'),'normal')
        --     ELSE GET_JSON_OBJECT(properties,'$.rec_type')
        -- END                                                                 AS rec_type,
        IF(GET_JSON_OBJECT(properties,'$.rec_type') IS NOT NULL, 'rec_reply', 'normal') AS rec_type,
        GET_JSON_OBJECT(properties,'$.chat_type')                           AS chat_type,
        GET_JSON_OBJECT(properties,'$.clk_type')                            AS clk_type,
        GET_JSON_OBJECT(properties,'$.call_status')                         AS call_status,
        GET_JSON_OBJECT(properties,'$.user_or_npc')                         AS user_or_npc,
        GET_JSON_OBJECT(properties,'$.msg_type')                            AS msg_type,
        GET_JSON_OBJECT(properties,'$.base_free_duration')                  AS base_free_duration,
        GET_JSON_OBJECT(properties,'$.deck_bonus_duration')                 AS deck_bonus_duration,
        GET_JSON_OBJECT(properties,'$.voice_call_id')                       AS voice_call_id,
        GET_JSON_OBJECT(properties,'$.call_round_id')                       AS call_round_id,
        GET_JSON_OBJECT(properties,'$.left_call_time')                      AS left_call_time,
        GET_JSON_OBJECT(properties,'$.call_duration')                       AS call_duration,
        GET_JSON_OBJECT(properties,'$.action_type')                         AS action_type,
        GET_JSON_OBJECT(properties,'$.feedback_type')                       AS feedback_type,
        GET_JSON_OBJECT(properties,'$.feedback_id')                         AS feedback_id,
        GET_JSON_OBJECT(properties,'$.item_type')                           AS item_type,
        GET_JSON_OBJECT(properties, '$.duration')                           AS duration,
        CASE GET_JSON_OBJECT(properties, '$.chat_scene')
            WHEN 0 THEN '单聊'
            WHEN 1 THEN '剧情聊天'
            WHEN 2 THEN '群聊'
        END                                                                 AS chat_scene
    FROM
        dwd_xingye.xingye_app_log_di
    WHERE
        ymd = '{p_date}'
        AND event_name IN (
            'message_send',
            'retalk_pick_click',
            'retalk_write_comfort_click',
            'msg_send',
            'call_end_status',
            'chat_rec_click',
            'retalk_msg_click',
            'retalk_write_click',
            'msg_action',
            'chat_reset_success',
            'msg_score_click',
            'message_receive',
            'chat_rec_result'
        )
        AND app_id = 600
)
INSERT OVERWRITE TABLE dialogue.dwd_xingye_did_client_chat_stats_di PARTITION(ymd = '{P_DATE}')
SELECT
    device_id,
    NVL(SUM(IF(metric_name = 'normal_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                              AS normal_msg_cnt_1d,
    NVL(SUM(IF(metric_name = 'single_chat_user_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                    AS single_chat_user_msg_cnt_1d,
    NVL(SUM(IF(metric_name = 'group_chat_user_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                     AS group_chat_user_msg_cnt_1d,
    NVL(SUM(IF(metric_name = 'story_chat_user_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                     AS story_chat_user_msg_cnt_1d,
    NVL(SUM(IF(metric_name = 'sgst_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                                AS sgst_msg_cnt_1d,
    NVL(SUM(IF(metric_name = 'resay_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                               AS resay_msg_cnt_1d,
    NVL(SUM(IF(metric_name = 'rewrite_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                             AS rewrite_msg_cnt_1d,
    NVL(SUM(IF(metric_name = 'voice_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                               AS voice_msg_cnt_1d,
    NVL(SUM(IF(metric_name = 'voice_cnt_1d', BIGINT(metric_value), 0)), 0)                                   AS voice_cnt_1d,
    NVL(SUM(IF(metric_name = 'voice_suc_cnt_1d', BIGINT(metric_value), 0)), 0)                               AS voice_suc_cnt_1d,
    NVL(SUM(IF(metric_name = 'voice_user_start_cnt_1d', BIGINT(metric_value), 0)), 0)                        AS voice_user_start_cnt_1d,
    NVL(SUM(IF(metric_name = 'voice_free_duration_cost_1d', BIGINT(metric_value), 0)), 0)                    AS voice_free_duration_cost_1d,
    -- NVL(SUM(IF(metric_name = 'voice_free_bonus_duration_cost_1d', BIGINT(metric_value), 0)), 0)              AS voice_free_bonus_duration_cost_1d,
    NVL(SUM(IF(metric_name = 'voice_call_duration_1d', BIGINT(metric_value), 0)), 0)                         AS voice_call_duration_1d,
    NVL(SUM(IF(metric_name = 'group_cnt_1d', BIGINT(metric_value), 0)), 0)                                   AS group_cnt_1d,
    NVL(SUM(IF(metric_name = 'group_chat_ai_msg_cnt_1d', BIGINT(metric_value), 0)), 0)                       AS group_chat_ai_msg_cnt_1d,
    SPLIT(NVL(MAX(IF(metric_name = 'single_chat_user_msg_cnt_list_1d', metric_value, '')), ''), ',')         AS single_chat_user_msg_cnt_list_1d,
    SPLIT(NVL(MAX(IF(metric_name = 'group_chat_user_msg_cnt_list_1d', metric_value, '')), ''), ',')          AS group_chat_user_msg_cnt_list_1d,
    SPLIT(NVL(MAX(IF(metric_name = 'group_chat_total_msg_cnt_list_1d', metric_value, '')), ''), ',')         AS group_chat_total_msg_cnt_list_1d,
    SPLIT(NVL(MAX(IF(metric_name = 'story_chat_user_msg_cnt_list_1d', metric_value, '')), ''), ',')          AS story_chat_user_msg_cnt_list_1d,
    SPLIT(NVL(MAX(IF(metric_name = 'voice_chat_user_msg_cnt_list_1d', metric_value, '')), ''), ',')          AS voice_chat_user_msg_cnt_list_1d,
    NVL(SUM(IF(metric_name = 'story_cnt_1d', BIGINT(metric_value), 0)), 0)                                   AS story_cnt_1d,
    NVL(SUM(IF(metric_name = 'sgst_clk_cnt_1d', BIGINT(metric_value), 0)), 0)                                AS sgst_clk_cnt_1d,
    NVL(SUM(IF(metric_name = 'resay_clk_cnt_1d', BIGINT(metric_value), 0)), 0)                               AS resay_clk_cnt_1d,
    NVL(SUM(IF(metric_name = 'rewrite_clk_cnt_1d', BIGINT(metric_value), 0)), 0)                             AS rewrite_clk_cnt_1d,
    NVL(SUM(IF(metric_name = 'backtrack_cnt_1d', BIGINT(metric_value), 0)), 0)                               AS backtrack_cnt_1d,
    NVL(SUM(IF(metric_name = 'reset_cnt_1d', BIGINT(metric_value), 0)), 0)                                   AS reset_cnt_1d,
    NVL(SUM(IF(metric_name = 'msg_score_1_cnt_1d', BIGINT(metric_value), 0)), 0)                             AS msg_score_1_cnt_1d,
    NVL(SUM(IF(metric_name = 'msg_score_2_cnt_1d', BIGINT(metric_value), 0)), 0)                             AS msg_score_2_cnt_1d,
    NVL(SUM(IF(metric_name = 'msg_score_3_cnt_1d', BIGINT(metric_value), 0)), 0)                             AS msg_score_3_cnt_1d,
    NVL(SUM(IF(metric_name = 'msg_score_4_cnt_1d', BIGINT(metric_value), 0)), 0)                             AS msg_score_4_cnt_1d,
    NVL(SUM(IF(metric_name = 'msg_score_5_cnt_1d', BIGINT(metric_value), 0)), 0)                             AS msg_score_5_cnt_1d,
    NVL(SUM(IF(metric_name = 'msg_score_6_cnt_1d', BIGINT(metric_value), 0)), 0)                             AS msg_score_6_cnt_1d,
    NVL(SUM(IF(metric_name = 'voice_score_1_cnt_1d', BIGINT(metric_value), 0)), 0)                           AS voice_score_1_cnt_1d,
    NVL(SUM(IF(metric_name = 'voice_score_2_cnt_1d', BIGINT(metric_value), 0)), 0)                           AS voice_score_2_cnt_1d,
    NVL(SUM(IF(metric_name = 'feedback_cnt_1d', BIGINT(metric_value), 0)), 0)                                AS feedback_cnt_1d,
    NVL(SUM(IF(metric_name = 'share_cnt_1d', BIGINT(metric_value), 0)), 0)                                   AS share_cnt_1d,
    NVL(SUM(IF(metric_name = 'chat_npc_cnt_1d', BIGINT(metric_value), 0)), 0)                                AS chat_npc_cnt_1d,
    NVL(SUM(IF(metric_name = 'single_ai_reply_ts', BIGINT(metric_value), 0)), 0)                             AS single_ai_reply_ts,
    NVL(SUM(IF(metric_name = 'single_ai_reply_cnt', BIGINT(metric_value), 0)), 0)                            AS single_ai_reply_cnt,
    NVL(SUM(IF(metric_name = 'single_ai_reply_ts_drt_5', BIGINT(metric_value), 0)), 0)                       AS single_ai_reply_ts_drt_5,
    NVL(SUM(IF(metric_name = 'single_ai_reply_ts_drt_20', BIGINT(metric_value), 0)), 0)                      AS single_ai_reply_ts_drt_20,
    NVL(SUM(IF(metric_name = 'single_ai_reply_ts_drt_30', BIGINT(metric_value), 0)), 0)                      AS single_ai_reply_ts_drt_30,
    NVL(SUM(IF(metric_name = 'single_ai_reply_ts_drt_50', BIGINT(metric_value), 0)), 0)                      AS single_ai_reply_ts_drt_50,
    NVL(SUM(IF(metric_name = 'single_ai_reply_ts_drt_80', BIGINT(metric_value), 0)), 0)                      AS single_ai_reply_ts_drt_80,
    NVL(SUM(IF(metric_name = 'single_ai_reply_ts_drt_90', BIGINT(metric_value), 0)), 0)                      AS single_ai_reply_ts_drt_90,
    NVL(SUM(IF(metric_name = 'single_ai_reply_ts_drt_95', BIGINT(metric_value), 0)), 0)                      AS single_ai_reply_ts_drt_95,
    NVL(SUM(IF(metric_name = 'story_ai_reply_ts', BIGINT(metric_value), 0)), 0)                              AS story_ai_reply_ts,
    NVL(SUM(IF(metric_name = 'story_ai_reply_cnt', BIGINT(metric_value), 0)), 0)                             AS story_ai_reply_cnt,
    NVL(SUM(IF(metric_name = 'story_ai_reply_ts_drt_5', BIGINT(metric_value), 0)), 0)                        AS story_ai_reply_ts_drt_5,
    NVL(SUM(IF(metric_name = 'story_ai_reply_ts_drt_20', BIGINT(metric_value), 0)), 0)                       AS story_ai_reply_ts_drt_20,
    NVL(SUM(IF(metric_name = 'story_ai_reply_ts_drt_30', BIGINT(metric_value), 0)), 0)                       AS story_ai_reply_ts_drt_30,
    NVL(SUM(IF(metric_name = 'story_ai_reply_ts_drt_50', BIGINT(metric_value), 0)), 0)                       AS story_ai_reply_ts_drt_50,
    NVL(SUM(IF(metric_name = 'story_ai_reply_ts_drt_80', BIGINT(metric_value), 0)), 0)                       AS story_ai_reply_ts_drt_80,
    NVL(SUM(IF(metric_name = 'story_ai_reply_ts_drt_90', BIGINT(metric_value), 0)), 0)                       AS story_ai_reply_ts_drt_90,
    NVL(SUM(IF(metric_name = 'story_ai_reply_ts_drt_95', BIGINT(metric_value), 0)), 0)                       AS story_ai_reply_ts_drt_95,
    NVL(SUM(IF(metric_name = 'group_ai_reply_ts', BIGINT(metric_value), 0)), 0)                              AS group_ai_reply_ts,
    NVL(SUM(IF(metric_name = 'group_ai_reply_cnt', BIGINT(metric_value), 0)), 0)                             AS group_ai_reply_cnt,
    NVL(SUM(IF(metric_name = 'group_ai_reply_ts_drt_5', BIGINT(metric_value), 0)), 0)                        AS group_ai_reply_ts_drt_5,
    NVL(SUM(IF(metric_name = 'group_ai_reply_ts_drt_20', BIGINT(metric_value), 0)), 0)                       AS group_ai_reply_ts_drt_20,
    NVL(SUM(IF(metric_name = 'group_ai_reply_ts_drt_30', BIGINT(metric_value), 0)), 0)                       AS group_ai_reply_ts_drt_30,
    NVL(SUM(IF(metric_name = 'group_ai_reply_ts_drt_50', BIGINT(metric_value), 0)), 0)                       AS group_ai_reply_ts_drt_50,
    NVL(SUM(IF(metric_name = 'group_ai_reply_ts_drt_80', BIGINT(metric_value), 0)), 0)                       AS group_ai_reply_ts_drt_80,
    NVL(SUM(IF(metric_name = 'group_ai_reply_ts_drt_90', BIGINT(metric_value), 0)), 0)                       AS group_ai_reply_ts_drt_90,
    NVL(SUM(IF(metric_name = 'group_ai_reply_ts_drt_95', BIGINT(metric_value), 0)), 0)                       AS group_ai_reply_ts_drt_95,
    NVL(SUM(IF(metric_name = 'suggest_reply_ts', BIGINT(metric_value), 0)), 0)                               AS suggest_reply_ts,
    NVL(SUM(IF(metric_name = 'suggest_reply_cnt', BIGINT(metric_value), 0)), 0)                              AS suggest_reply_cnt,
    NVL(SUM(IF(metric_name = 'suggest_reply_ts_drt_5', BIGINT(metric_value), 0)), 0)                         AS suggest_reply_ts_drt_5,
    NVL(SUM(IF(metric_name = 'suggest_reply_ts_drt_20', BIGINT(metric_value), 0)), 0)                        AS suggest_reply_ts_drt_20,
    NVL(SUM(IF(metric_name = 'suggest_reply_ts_drt_30', BIGINT(metric_value), 0)), 0)                        AS suggest_reply_ts_drt_30,
    NVL(SUM(IF(metric_name = 'suggest_reply_ts_drt_50', BIGINT(metric_value), 0)), 0)                        AS suggest_reply_ts_drt_50,
    NVL(SUM(IF(metric_name = 'suggest_reply_ts_drt_80', BIGINT(metric_value), 0)), 0)                        AS suggest_reply_ts_drt_80,
    NVL(SUM(IF(metric_name = 'suggest_reply_ts_drt_90', BIGINT(metric_value), 0)), 0)                        AS suggest_reply_ts_drt_90,
    NVL(SUM(IF(metric_name = 'suggest_reply_ts_drt_95', BIGINT(metric_value), 0)), 0)                        AS suggest_reply_ts_drt_95
FROM
(
    SELECT
        device_id,
        metric_name,
        STRING(metric_value) AS metric_value
    FROM
    (
        SELECT  
            device_id,
            MAP(
                'normal_msg_cnt_1d'                 ,normal_msg_cnt_1d,
                'single_chat_user_msg_cnt_1d'       ,single_chat_user_msg_cnt_1d,
                'group_chat_user_msg_cnt_1d'        ,group_chat_user_msg_cnt_1d,
                'story_chat_user_msg_cnt_1d'        ,story_chat_user_msg_cnt_1d,
                'sgst_msg_cnt_1d'                   ,sgst_msg_cnt_1d,
                'resay_msg_cnt_1d'                  ,resay_msg_cnt_1d,
                'rewrite_msg_cnt_1d'                ,rewrite_msg_cnt_1d,
                'voice_msg_cnt_1d'                  ,voice_msg_cnt_1d,
                'voice_cnt_1d'                      ,voice_cnt_1d,
                'voice_suc_cnt_1d'                  ,voice_suc_cnt_1d,
                'voice_user_start_cnt_1d'           ,voice_user_start_cnt_1d,
                'group_cnt_1d'                      ,group_cnt_1d,
                'group_chat_ai_msg_cnt_1d'          ,group_chat_ai_msg_cnt_1d,
                'group_chat_user_msg_cnt_list_1d'   ,group_chat_user_msg_cnt_list_1d,
                'group_chat_msg_cnt_list_1d'        ,group_chat_msg_cnt_list_1d,
                'story_cnt_1d'                      ,story_cnt_1d,
                'sgst_clk_cnt_1d'                   ,sgst_clk_cnt_1d,
                'resay_clk_cnt_1d'                  ,resay_clk_cnt_1d,
                'rewrite_clk_cnt_1d'                ,rewrite_clk_cnt_1d,
                'backtrack_cnt_1d'                  ,backtrack_cnt_1d,
                'reset_cnt_1d'                      ,reset_cnt_1d,
                'msg_score_1_cnt_1d'                ,msg_score_1_cnt_1d,
                'msg_score_2_cnt_1d'                ,msg_score_2_cnt_1d,
                'msg_score_3_cnt_1d'                ,msg_score_3_cnt_1d,
                'msg_score_4_cnt_1d'                ,msg_score_4_cnt_1d,
                'msg_score_5_cnt_1d'                ,msg_score_5_cnt_1d,
                'msg_score_6_cnt_1d'                ,msg_score_6_cnt_1d,
                'voice_score_1_cnt_1d'              ,voice_score_1_cnt_1d,
                'voice_score_2_cnt_1d'              ,voice_score_2_cnt_1d,
                'feedback_cnt_1d'                   ,feedback_cnt_1d,
                'share_cnt_1d'                      ,share_cnt_1d,
                'chat_npc_cnt_1d'                   ,chat_npc_cnt_1d,
                'single_ai_reply_ts'                ,single_ai_reply_ts,
                'single_ai_reply_cnt'               ,single_ai_reply_cnt,
                'single_ai_reply_ts_drt_5'          ,single_ai_reply_ts_drt_5,
                'single_ai_reply_ts_drt_20'         ,single_ai_reply_ts_drt_20,
                'single_ai_reply_ts_drt_30'         ,single_ai_reply_ts_drt_30,
                'single_ai_reply_ts_drt_50'         ,single_ai_reply_ts_drt_50,
                'single_ai_reply_ts_drt_80'         ,single_ai_reply_ts_drt_80,
                'single_ai_reply_ts_drt_90'         ,single_ai_reply_ts_drt_90,
                'single_ai_reply_ts_drt_95'         ,single_ai_reply_ts_drt_95,
                'story_ai_reply_ts'                 ,story_ai_reply_ts,
                'story_ai_reply_cnt'                ,story_ai_reply_cnt,
                'story_ai_reply_ts_drt_5'           ,story_ai_reply_ts_drt_5,
                'story_ai_reply_ts_drt_20'          ,story_ai_reply_ts_drt_20,
                'story_ai_reply_ts_drt_30'          ,story_ai_reply_ts_drt_30,
                'story_ai_reply_ts_drt_50'          ,story_ai_reply_ts_drt_50,
                'story_ai_reply_ts_drt_80'          ,story_ai_reply_ts_drt_80,
                'story_ai_reply_ts_drt_90'          ,story_ai_reply_ts_drt_90,
                'story_ai_reply_ts_drt_95'          ,story_ai_reply_ts_drt_95,
                'group_ai_reply_ts'                 ,group_ai_reply_ts,
                'group_ai_reply_cnt'                ,group_ai_reply_cnt,
                'group_ai_reply_ts_drt_5'           ,group_ai_reply_ts_drt_5,
                'group_ai_reply_ts_drt_20'          ,group_ai_reply_ts_drt_20,
                'group_ai_reply_ts_drt_30'          ,group_ai_reply_ts_drt_30,
                'group_ai_reply_ts_drt_50'          ,group_ai_reply_ts_drt_50,
                'group_ai_reply_ts_drt_80'          ,group_ai_reply_ts_drt_80,
                'group_ai_reply_ts_drt_90'          ,group_ai_reply_ts_drt_90,
                'group_ai_reply_ts_drt_95'          ,group_ai_reply_ts_drt_95,
                'suggest_reply_ts'                  ,suggest_reply_ts,
                'suggest_reply_cnt'                 ,suggest_reply_cnt,
                'suggest_reply_ts_drt_5'            ,suggest_reply_ts_drt_5,
                'suggest_reply_ts_drt_20'           ,suggest_reply_ts_drt_20,
                'suggest_reply_ts_drt_30'           ,suggest_reply_ts_drt_30,
                'suggest_reply_ts_drt_50'           ,suggest_reply_ts_drt_50,
                'suggest_reply_ts_drt_80'           ,suggest_reply_ts_drt_80,
                'suggest_reply_ts_drt_90'           ,suggest_reply_ts_drt_90,
                'suggest_reply_ts_drt_95'           ,suggest_reply_ts_drt_95
            ) AS value_map   
        FROM
        (
            SELECT
                device_id,
                
                COUNT(DISTINCT IF(event_name = 'message_send' AND COALESCE(rec_type,'normal') = 'normal' AND COALESCE(msg_type, 'ugc') != 'aigc', message_id, NULL))                                                                     AS normal_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') NOT IN ('group_chat','story_chat') AND COALESCE(page, '') NOT IN ('groupchat_page','story_chat_page')), message_id, NULL))                                       AS single_chat_user_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'group_chat' OR page in ('groupchat_page')) AND COALESCE(msg_type, 'ugc') = 'ugc', message_id, NULL))                               AS group_chat_user_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'story_chat' OR page in ('story_chat_page')), message_id, NULL))                                                   AS story_chat_user_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND rec_type IN ('rec_reply','rec_aside','rec_bubble','rec_asside'), message_id, NULL))                                            AS sgst_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'retalk_pick_click', message_id, NULL))                                                                                                           AS resay_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'retalk_write_comfort_click' AND clk_type = '1', message_id, NULL))                                                                               AS rewrite_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_send', call_round_id, NULL))                                                                                                                 AS voice_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'call_end_status', voice_call_id, NULL))                                                                                                          AS voice_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'call_end_status' AND call_status = 'success', voice_call_id, NULL))                                                                              AS voice_suc_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'call_end_status' AND user_or_npc = 'user', voice_call_id, NULL))                                                                                 AS voice_user_start_cnt_1d,
                SUM(IF(event_name = 'call_end_status', base_free_duration, NULL))                                                                                                                AS voice_free_duration_cost_1d,
                -- SUM(IF(event_name = 'call_end_status', deck_bonus_duration, NULL))                                                                                                               AS voice_free_bonus_duration_cost_1d,
                SUM(IF(event_name = 'call_end_status', call_duration, NULL))                                                                                                                     AS voice_call_duration_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'group_chat' OR page in ('groupchat_page')), chat_id, NULL))                                                       AS group_cnt_1d,
                COUNT(IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'group_chat' OR page in ('groupchat_page')) AND msg_type = 'aigc', 1, NULL))                                               AS group_chat_ai_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'group_chat' OR page in ('groupchat_page')) AND msg_type = 'ugc', message_id, NULL))                               AS group_chat_user_msg_cnt_list_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'group_chat' OR page in ('groupchat_page')), message_id, NULL))                                                    AS group_chat_msg_cnt_list_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'story_chat' OR page in ('story_chat_page')), chat_id, NULL))                                                      AS story_cnt_1d,
                COUNT(IF(event_name = 'chat_rec_click', 1, NULL))                                                                                                                                AS sgst_clk_cnt_1d,
                COUNT(IF(event_name = 'retalk_msg_click', 1, NULL))                                                                                                                              AS resay_clk_cnt_1d,
                COUNT(IF(event_name = 'retalk_write_click', 1, NULL))                                                                                                                            AS rewrite_clk_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_action' AND action_type = 'backtrack', message_id, NULL))                                                                                    AS backtrack_cnt_1d,
                COUNT(IF(event_name = 'chat_reset_success', 1, NULL))                                                                                                                            AS reset_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_score_click' AND feedback_type = 'normal_chat' AND feedback_id = '1' AND COALESCE(item_type, 'confirm') = 'confirm', message_id, NULL))      AS msg_score_1_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_score_click' AND feedback_type = 'normal_chat' AND feedback_id = '2' AND COALESCE(item_type, 'confirm') = 'confirm', message_id, NULL))      AS msg_score_2_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_score_click' AND feedback_type = 'normal_chat' AND feedback_id = '3' AND COALESCE(item_type, 'confirm') = 'confirm', message_id, NULL))      AS msg_score_3_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_score_click' AND feedback_type = 'normal_chat' AND feedback_id = '4' AND COALESCE(item_type, 'confirm') = 'confirm', message_id, NULL))      AS msg_score_4_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_score_click' AND feedback_type = 'normal_chat' AND feedback_id = '5' AND COALESCE(item_type, 'confirm') = 'confirm', message_id, NULL))      AS msg_score_5_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_score_click' AND feedback_type = 'normal_chat' AND feedback_id = '6' AND COALESCE(item_type, 'confirm') = 'confirm', message_id, NULL))      AS msg_score_6_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_score_click' AND feedback_type = 'call_chat' AND feedback_id = '1' AND COALESCE(item_type, 'confirm') = 'confirm', voice_call_id, NULL))     AS voice_score_1_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_score_click' AND feedback_type = 'call_chat' AND feedback_id = '2' AND COALESCE(item_type, 'confirm') = 'confirm', voice_call_id, NULL))     AS voice_score_2_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_action' AND action_type IN ('2', 'feedback'), message_id, NULL))                                                                             AS feedback_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'msg_action' AND action_type = 'share', message_id, NULL))                                                                                        AS share_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND BIGINT(npc_id) > 0, npc_id, NULL))                                                                                             AS chat_npc_cnt_1d,
                
                SUM(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL))                           AS single_ai_reply_ts,
                COUNT(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL))                         AS single_ai_reply_cnt,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL), 0.05)       AS single_ai_reply_ts_drt_5,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL), 0.2)        AS single_ai_reply_ts_drt_20,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL), 0.30)       AS single_ai_reply_ts_drt_30,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL), 0.50)       AS single_ai_reply_ts_drt_50,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL), 0.80)       AS single_ai_reply_ts_drt_80,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL), 0.90)       AS single_ai_reply_ts_drt_90,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '单聊', duration, NULL), 0.95)       AS single_ai_reply_ts_drt_95,
                
                SUM(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL))                        AS story_ai_reply_ts,
                COUNT(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL))                      AS story_ai_reply_cnt,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL), 0.05)    AS story_ai_reply_ts_drt_5,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL), 0.2)     AS story_ai_reply_ts_drt_20,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL), 0.30)    AS story_ai_reply_ts_drt_30,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL), 0.50)    AS story_ai_reply_ts_drt_50,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL), 0.80)    AS story_ai_reply_ts_drt_80,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL), 0.90)    AS story_ai_reply_ts_drt_90,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '剧情聊天', duration, NULL), 0.95)    AS story_ai_reply_ts_drt_95,
                
                SUM(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL))                           AS group_ai_reply_ts,
                COUNT(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL))                         AS group_ai_reply_cnt,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL), 0.05)       AS group_ai_reply_ts_drt_5,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL), 0.2)        AS group_ai_reply_ts_drt_20,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL), 0.30)       AS group_ai_reply_ts_drt_30,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL), 0.50)       AS group_ai_reply_ts_drt_50,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL), 0.80)       AS group_ai_reply_ts_drt_80,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL), 0.90)       AS group_ai_reply_ts_drt_90,
                APPROX_PERCENTILE(IF(event_name = 'message_receive' AND duration < 10000 AND chat_scene = '群聊', duration, NULL), 0.95)       AS group_ai_reply_ts_drt_95,
                
                SUM(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL))                                                   AS suggest_reply_ts,
                COUNT(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL))                                                 AS suggest_reply_cnt,
                APPROX_PERCENTILE(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL), 0.05)                               AS suggest_reply_ts_drt_5,
                APPROX_PERCENTILE(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL), 0.2)                                AS suggest_reply_ts_drt_20,
                APPROX_PERCENTILE(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL), 0.30)                               AS suggest_reply_ts_drt_30,
                APPROX_PERCENTILE(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL), 0.50)                               AS suggest_reply_ts_drt_50,
                APPROX_PERCENTILE(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL), 0.80)                               AS suggest_reply_ts_drt_80,
                APPROX_PERCENTILE(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL), 0.90)                               AS suggest_reply_ts_drt_90,
                APPROX_PERCENTILE(IF(event_name = 'chat_rec_result' AND duration < 10000, duration, NULL), 0.95)                               AS suggest_reply_ts_drt_95

            FROM
                base_data
            WHERE
                event_name IN (
                    'message_send',
                    'retalk_pick_click',
                    'retalk_write_comfort_click',
                    'msg_send',
                    'call_end_status',
                    'chat_rec_click',
                    'retalk_msg_click',
                    'retalk_write_click',
                    'msg_action',
                    'chat_reset_success',
                    'msg_score_click',
                    'message_receive',
                    'chat_rec_result'
                )
            GROUP BY
                device_id
        )

    ) LATERAL VIEW EXPLODE(value_map) AS metric_name , metric_value    
    
    UNION all
    
    SELECT
        device_id,
        metric_name,
        STRING(metric_value) AS metric_value
    FROM
    (
        SELECT  
            device_id,
            MAP(
                'voice_free_duration_cost_1d'           ,300000 - MIN(base_free_duration),
                -- 'voice_free_bonus_duration_cost_1d'     ,SUM(deck_bonus_duration),
                'voice_call_duration_1d'                ,SUM(call_duration)
            ) AS value_map   
        FROM
        (
            SELECT
                device_id,
                voice_call_id,
                MAX(base_free_duration)     AS base_free_duration,
                -- MAX(deck_bonus_duration)    AS deck_bonus_duration,
                MAX(call_duration)          AS call_duration
            FROM
                base_data
            WHERE
                event_name IN (
                    'call_end_status'
                )
                AND BIGINT(DOUBLE(voice_call_id)) > 0
            GROUP BY
                device_id,
                voice_call_id
        )
        GROUP BY 
            device_id
    ) LATERAL VIEW EXPLODE(value_map) AS metric_name , metric_value        
    
    UNION all
    
    SELECT
        device_id,
        metric_name,
        ARRAY_JOIN(metric_value, ',') AS metric_value
    FROM
    (
        SELECT  
            device_id,
            MAP(
                'single_chat_user_msg_cnt_list_1d'               ,array_remove(COLLECT_LIST(single_chat_user_msg_cnt_1d),0),
                'group_chat_user_msg_cnt_list_1d'                ,array_remove(COLLECT_LIST(group_chat_user_msg_cnt_1d),0),
                'group_chat_total_msg_cnt_list_1d'               ,array_remove(COLLECT_LIST(group_chat_total_msg_cnt_1d),0),
                'story_chat_user_msg_cnt_list_1d'                ,array_remove(COLLECT_LIST(story_chat_user_msg_cnt_1d),0)
            ) AS value_map   
        FROM
        (
            SELECT
                device_id,
                chat_id,
                
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') NOT IN ('group_chat','story_chat') AND COALESCE(page, '') NOT IN ('groupchat_page','story_chat_page')), message_id, NULL))              AS single_chat_user_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'group_chat' OR page in ('groupchat_page')) AND COALESCE(msg_type, 'ugc') = 'ugc', message_id, NULL))                                 AS group_chat_user_msg_cnt_1d,
                COUNT(IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'group_chat' OR page in ('groupchat_page')), 1, NULL))                                                                                         AS group_chat_total_msg_cnt_1d,
                COUNT(DISTINCT IF(event_name = 'message_send' AND (COALESCE(chat_type, 'single_chat') = 'story_chat' OR page in ('story_chat_page')), message_id, NULL))                                                                      AS story_chat_user_msg_cnt_1d

            FROM
                base_data
            WHERE
                event_name IN (
                    'message_send'
                )
                AND COALESCE(chat_id,'') !=''
            GROUP BY
                device_id,
                chat_id
        )
        GROUP BY 
            device_id
    ) LATERAL VIEW EXPLODE(value_map) AS metric_name , metric_value    

    UNION all
    
    SELECT
        device_id,
        metric_name,
        ARRAY_JOIN(metric_value, ',') AS metric_value
    FROM
    (
        SELECT  
            device_id,
            MAP(
                'voice_chat_user_msg_cnt_list_1d'               ,array_remove(COLLECT_LIST(voice_chat_user_msg_cnt_1d),0)
            ) AS value_map   
        FROM
        (
            SELECT
                device_id,
                voice_call_id,
                COUNT(DISTINCT call_round_id)          AS voice_chat_user_msg_cnt_1d
            FROM
                base_data
            WHERE
                event_name IN (
                    'msg_send'
                )
                AND COALESCE(voice_call_id,'') !=''
            GROUP BY
                device_id,
                voice_call_id
        )
        GROUP BY 
            device_id
    ) LATERAL VIEW EXPLODE(value_map) AS metric_name , metric_value
)
GROUP BY
    device_id
;
    """

    print(sql)
    spark.sql(sql)

    standardized_quality_check({
        'key_columns': ['device_id'],
        'partition_date': P_DATE,
        'table_name': 'dialogue.dwd_xingye_did_client_chat_stats_di'
    }, spark)

    spark.stop()