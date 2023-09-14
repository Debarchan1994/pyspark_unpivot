This function can be used to unpivot any number of columns in your Spark Dataframe. This code also shows how to merge your unpivoted data frame with the main data frame and how to use aggregations on top of that.

NOTE: If you use the unpivot operation and merge it with the main data frame, it will increase the number of rows by (number of rows in the original data frame before unpivot * number of columns unpivoted). It will result in the repetition of 
values for all the columns expect for the columns that are unpivoted.
