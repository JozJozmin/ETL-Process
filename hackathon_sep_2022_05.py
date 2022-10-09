
# Import libraries
from pyspark.sql.functions import *

# Your code goes inside this function
# Function input - spark object, click data path, resolved data path
# Function output - final spark dataframe
def sample_function(spark, s3_clickstream_path, s3_login_path):

    df_clickstream =  spark.read.format("json").load(s3_clickstream_path)
    user_mapping =  spark.read.format("csv").option("header",True).load(s3_login_path)

    df_clickstream.createOrReplaceTempView("df_clickstream")
    user_mapping.createOrReplaceTempView("user_mapping")

    ### joining the two tables 
    user_mapping_clickstream = spark.sql(
        """ SELECT df_clickstream.browser_id,     
                user_mapping.user_id,user_mapping.session_id,      
                SUBSTRING(df_clickstream.event_date_time,1,10) AS current_date,
                CASE
                    WHEN df_clickstream.event_type = 'pageload' THEN 1 
                    ELSE 0
                    END AS page_load_col,
                    CASE 
                        WHEN df_clickstream.event_type = 'click' THEN 1
                        ELSE 0
                        END AS click_col,     
                    CASE
                        WHEN user_mapping.user_id  IS NULL THEN 0
                        ELSE 1
                        END AS logged_in
                FROM df_clickstream 
                FULL OUTER JOIN user_mapping 
                ON (df_clickstream.session_id = user_mapping.session_id)
                """)

    ### finding out the number of clicks and number of pageloads from user_mapping_clickstream dataframe
    user_mapping_clickstream1 = user_mapping_clickstream.groupby('browser_id','user_id','current_date','session_id','logged_in').agg(sum('page_load_col').alias('number_of_clicks')                                                   
                                                                            ,sum('click_col').alias('number_of_pageloads')) 
    ### filter data between August 1 & August 10
   # user_mapping_clickstream1 = user_mapping_clickstream1.filter((user_mapping_clickstream1.current_date <= '2022-08-10') & (user_mapping_clickstream1.current_date >= '2022-08-01'))

    ## creating a dataframe for finding out the first url
    first_url_df = spark.sql("""(select current_date, 
                            browser_id,
                            user_id,
                            client_side_data.current_page_url AS first_url
                            from 
                            (select *,SUBSTRING(event_date_time,1,10) AS current_date,
                            row_number() 
                            OVER (PARTITION BY SUBSTRING(event_date_time,1,10),browser_id,user_id
                            ORDER BY event_date_time ASC,browser_id) as rn 
                            FROM df_clickstream
                            LEFT JOIN user_mapping 
                            ON df_clickstream.session_id = user_mapping.session_id
                            )tmp where rn = 1)""")
    ### filtering the data only between August 1 & August 10
    #first_url_df = first_url_df.filter((first_url_df.current_date <= '2022-08-10') & (first_url_df.current_date >= '2022-08-01'))

    ### joining user_mapping_clickstream1 & first_url_df
    df_result = user_mapping_clickstream1.join(first_url_df, 
                    on=['browser_id','user_id','current_date'], how='left')

    df_result = df_result.select(['current_date',
                           'browser_id',
                            'user_id',
                            'logged_in',
                            'first_url',
                             'number_of_clicks',
                             'number_of_pageloads',
                             ])
    ## filtering null values in the browser_id column                        
    #df_result = df_result.filter(~df_result.browser_id.isNull())
    # Return your final spark df
    return df_result

    
