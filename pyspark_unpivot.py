from pyspark.sql.functions import col, lag, lead, isnull, concat, when, row_number, last, sum, lit, format_string, to_timestamp, unix_timestamp, dayofweek, hour, countDistinct
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, IntegerType, DoubleType, ArrayType, BooleanType, LongType

def dataframe(parent_dataframe):

    # Map the dataframe
    df = parent_dataframe


    # Define the labels for the bins
    

    x_cat = ['class1', 'class2', 'class3']

    x_label_array = F.array(*(F.lit(label) for label in x_cat))

    
    df_meta = df

    
    ########Unpivot the data frame
   

    # Define the stack function to unpivot the table. The number changes according to the number of columns you want to stack                   
    unpivotExpr = "stack(3, '1', column_1, \
                            '2', column_2, \
                            '3', column_3, \
                            ) as (Operating_Point, value_)"

    df_unpivot = df.select("put_here_a_unique_column", F.expr(unpivotExpr))

    df_unpivot = df_unpivot \
                                .withColumn('class', F.col('Operating_Point').cast(IntegerType())) \
                                .withColumn('value_new', F.round(F.col('value_'), 3)) \
                                .withColumn("class_label", x_label_array.getItem(F.col("class").cast("integer")-1))


    
    # Merge the dataframe and aggregate the data
   

    df_merge = df_meta.join(df_unpivot, 'common_unique_column_between_2_tables', 'inner')

    
    ## use aggregation if required
    
    df_agg = df_merge \
                        .groupBy('column_x', 'column_y', 'column_z') \
                        .agg(F.round(F.sum('value_new'), 3).alias('summed_value_new'),
                            F.countDistinct('common_unique_column_between_2_tables').alias('total_value'),
                            F.countDistinct(F.when(F.col('value_new') > 0, F.col('common_unique_column_between_2_tables')).otherwise(None)).alias('total_value_true'))
    
 
    
    return df_agg

    
    
