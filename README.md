This function can be used to unpivot any number of columns in your Spark Dataframe. This code also shows how to merge your unpivoted data frame with the main data frame and how to use aggregations on top of that.
NOTE: If you using unpivot operation and merging it with the main data frame, it will increase number of rows by (number of rows in the original data frame before unpivot * number of column unpivoted). It will result in the repetation of 
values for all the column expect the for the column that are unpivoted.
