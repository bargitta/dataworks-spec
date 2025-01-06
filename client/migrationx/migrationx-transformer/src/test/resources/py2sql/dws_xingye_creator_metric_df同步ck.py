from pyspark.sql import SparkSession
# from da_utils import *
import sys

if __name__ == '__main__':
    p_date = sys.argv[1]
    # P_DATE = date_upper(p_date)
    P_DATE = p_date

    spark = SparkSession.builder.getOrCreate()

    sql = f"""
   WITH device_table AS
(
    xxxx
),

category_table AS 
(
    SELECT 
        user_id,
        category AS category
    FROM 
    (    
       xxx
    )
    WHERE 
        rank = 1 
        AND category_percentage >=0.7
),

INSERT OVERWRITE da.dwd_xingye_user_portrait_df PARTITION(ymd = '{P_DATE}')
SELECT 
    'xingye'    AS  app_name,
    NVL(t10.nick_name,'')                      AS  nick_name
FROM 
(
    SELECT
        id          AS user_id
    FROM
        ods_xingye.account_account_df
    WHERE
        ymd = GREATEST('{p_date}', '20230907')
        AND FROM_UNIXTIME(created_at / 1000, 'yyyy-MM-dd') <= '{P_DATE}'
    GROUP BY 
        id
)t
LEFT JOIN 
(
    SELECT 
        user_id,
        FROM 
            da.dwm_xingye_npc_meta_metric_di
        WHERE
            ymd = '{P_DATE}'
    )t 
    GROUP BY 
        user_id 
)t1
ON t.user_id = t1.user_id 
LEFT JOIN (
    SELECT 
LEFT JOIN 
    maintained_creator t10
ON t.user_id = t10.user_id     
;
    """
    print(sql)
    spark.sql(sql)

    # standardized_quality_check({
    #     'key_columns': ['user_id'],
    #     'partition_date': P_DATE,
    #     'table_name': 'da.dwd_xingye_user_portrait_df'
    # }, spark)

    spark.stop()
