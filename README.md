this project was created as a result of series of excersises for Objected Oriented Programming course
it was written in `Java 8`

It has implemented following fearures:
+ a basic Dataframe for storing typed data in named columns
+ a "sparse" dataframe allowing to save memory by not storing a duplicate entry - it also has a memory optimization algorythm 
that can be run on demand to select the best value to be a designated duplicate
+ a grouping system - allowing to reduce a group of data into a single value
+ basic reductions like min or average value for the grouping system
+ a wrapper for a database connection allowing to use most Dataframe based algorythms on a MySQL database 
+ a server - client solution for computing the reductions on dataframes - allowing less powerfull hardware to have access to data
