// Databricks notebook source
// DBTITLE 1,Loading the raw data
val df1 = spark.read.format("csv").option("header",true).option("inferSchema",true).load("dbfs:/FileStore/shared_uploads/Satadru1998@gmail.com/Google_playstore_data.csv")

// COMMAND ----------

// DBTITLE 1,Data Inspection
df1.show(5)

// COMMAND ----------

// DBTITLE 1,Datatype of Columns
df1.printSchema()

// COMMAND ----------

// MAGIC %md # Installs , Price should be Numerical datatype but SPARK is infering as String..Strange!!

// COMMAND ----------

// DBTITLE 1,Unique values of Category
val df2=df1.select("Category").distinct();
df2.show(df2.count().toInt)

// COMMAND ----------

// MAGIC %md # 1.9 Category !! Strange

// COMMAND ----------

// DBTITLE 1,Let's check unique values of Suspicious Installs Column
val df2=df1.select("Installs").distinct();
df2.show(df2.count().toInt)

// COMMAND ----------

// MAGIC %md # Spark is infering Installs column as String because of "Free" value present in this column

// COMMAND ----------

// DBTITLE 1,Distinct values of Type column
val df2=df1.select("Type").distinct();
df2.show(df2.count().toInt)

// COMMAND ----------

// MAGIC %md  # Type should be only Free or Paid

// COMMAND ----------

// DBTITLE 1,Distinct values of Price column
val df2=df1.select("Price").distinct();
df2.show(df2.count().toInt)

// COMMAND ----------

// MAGIC %md # Seems Price value is in Dollar!! 
// MAGIC 
// MAGIC  # Price can not be "Everyone" :-)

// COMMAND ----------

val df2=df1.select("Content_Rating").distinct();
df2.show(df2.count().toInt)

// COMMAND ----------

// MAGIC %md # There is missing value in Content Rating Column

// COMMAND ----------

// DBTITLE 1,Unique Values present in Size column
val df2=df1.select("Size").distinct();
val df3=df2.sort("Size")
df3.show(df3.count().toInt)

// COMMAND ----------

// MAGIC %md  # There is missing value in Size Column

// COMMAND ----------

// DBTITLE 1,Number of Columns in Data
df1.columns.size

// COMMAND ----------

// DBTITLE 1,Number of Rows in Data
df1.count()

// COMMAND ----------

// MAGIC %md  # Shape of the Data=(10841,13)

// COMMAND ----------

// MAGIC %md # Primary Filtering:App Name can not be Same !!

// COMMAND ----------

// DBTITLE 1,Check Duplicate entries based on App column
df1.select("App").distinct().count()

// COMMAND ----------

// DBTITLE 1,Removing Duplicate entries based on App column
val df2=df1.dropDuplicates("App");
df2.count()

// COMMAND ----------

val df1=df2;

// COMMAND ----------

// DBTITLE 1,Columns of Data
df1.columns

// COMMAND ----------

// DBTITLE 1,Checking for missing values
println("App: "+df1.filter(df1.col("App").isNull).count())
println("Category: "+df1.filter(df1.col("Category").isNull).count())
println("Rating: "+df1.filter(df1.col("Rating").isNull).count())
println("Reviews: "+df1.filter(df1.col("Reviews").isNull).count())
println("Size: "+df1.filter(df1.col("Size").isNull).count())
println("Installs: "+df1.filter(df1.col("Installs").isNull).count())
println("Type: "+df1.filter(df1.col("Type").isNull).count())
println("Price: "+df1.filter(df1.col("Price").isNull).count())
println("Content_Rating: "+df1.filter(df1.col("Content_Rating").isNull).count())
println("Genres: "+df1.filter(df1.col("Genres").isNull).count())
println("Last_Updated: "+df1.filter(df1.col("Last_Updated").isNull).count())
println("Current_Ver1: "+df1.filter(df1.col("Current_Ver").isNull).count())
println("Android_Ver: "+df1.filter(df1.col("Android_Ver").isNull).count())

// COMMAND ----------

// DBTITLE 1,Lets look into frequency of some item to get an idea of data nature
val pu=df1.select("Size");
display(pu)

// COMMAND ----------

// MAGIC %md #It is evident that most of the apps range between 0 to 100 MB

// COMMAND ----------

// MAGIC %md Another test is the QQ plot (quantile-quantile). This will also show us if our continuous variables have a normal distribution. We know that the distributions were not normally distributed by the histograms. The QQ plot here is for illustration purposes.
// MAGIC 
// MAGIC A QQ plot will display a diagonal straight line if it is normally distributed. In our observations we can quickly see that these variables are not normally distributed. 

// COMMAND ----------

// DBTITLE 1,QQ Plot for the Size of App column
val pu=df1.select("Size");
display(pu)

// COMMAND ----------

// MAGIC %md  As the dataset is completely skewed(Right/Positively Skewed) , so we are going to fill the missing values using median of the size column

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Calculate median for Size column
val df2=df1.select("Size").na.drop();
println(df2.stat.approxQuantile("Size",Array(0.5),0)(0))

// COMMAND ----------

// DBTITLE 1,Fill all the null values of Size column by median the column 
val ou=Map("Size"->14);
val df2=df1.na.fill(ou);

// COMMAND ----------

val df1=df2;

// COMMAND ----------

// MAGIC %md #Size Column Cleaning Done

// COMMAND ----------

// DBTITLE 1,App with Largest Size
val ps=df1.select(max(df1.col("Size")));
val ms=ps.first()(0);
val dg=df1.filter(df1.col("Size")===ms);
dg.select("App").show()

// COMMAND ----------

// MAGIC %md # "Word Search Tab 1 FR" is the App with the largest size

// COMMAND ----------

// DBTITLE 1,Handling the null values for Rating Column
val pu=df1.select("Rating");
display(pu)

// COMMAND ----------

// MAGIC %md It is clear outlier is present in the dataset . Here we are removing all the rows which has Apps rating greater than 5

// COMMAND ----------

// DBTITLE 1,Filtering all rows which have Rating lesser than or equal to 5
val df2=df1.filter(df1.col("Rating")<=5);

// COMMAND ----------

// DBTITLE 1,Check the size after filtering
val df1=df2;
df1.count()

// COMMAND ----------

// DBTITLE 1,Distribution after using filter on Rating column
val pu=df1.select("Rating");
display(pu)

// COMMAND ----------

// DBTITLE 1,All null values are removed 
df1.filter(df1.col("Reviews").isNull).count()

// COMMAND ----------

// DBTITLE 1,There are a lot of outliers present in the Rating Column
val pu=df1.select("Rating");
display(pu)

// COMMAND ----------

// DBTITLE 1,Suggestion
// MAGIC %md Square/Cube Transform

// COMMAND ----------

// DBTITLE 1,Inter Quartile Range
df1.stat.approxQuantile("Rating",Array(0.25,0.5,0.75),0)

// COMMAND ----------

// MAGIC %md #IQR for the Rating column is 4.5-4.0=0.5

// COMMAND ----------

// DBTITLE 1,Upper Limit
val upper_bound=4.5+1.5*0.5;
println("upper_bound :"+upper_bound);

// COMMAND ----------

// DBTITLE 1,Lower Limit
val lower_bound=4.0-1.5*0.5;
println("lower_bound :"+lower_bound);

// COMMAND ----------

// MAGIC %md # All Rating values greater than 5.25 and all rating values lesser than 3.25 are Outlier

// COMMAND ----------

// DBTITLE 1,Check null values for Reviews Column
df1.filter(df1.col("Reviews").isNull).count()

// COMMAND ----------

// DBTITLE 1,Maximum Rating App
val ps=df1.select(max(df1.col("Rating")));
val ms=ps.first()(0);
val dg=df1.filter(df1.col("Rating")===ms);
dg.show()

// COMMAND ----------

// MAGIC %md #Size & Rating Column Cleaning done

// COMMAND ----------

// DBTITLE 1,Let's check out the App categories
val uop=df1.select("Category").distinct();
uop.show(uop.count().toInt)

// COMMAND ----------

// MAGIC %md 1.9 Category is autometically removed

// COMMAND ----------

// MAGIC %md #Category column is pretty much cleaned (pretty much! Not complete clean! Guessing FAILY should be Family , GAE should be GAME etc)

// COMMAND ----------

// DBTITLE 1, Ratings per app category in a sequential order
val uap=df1.groupBy("Category").mean("Rating");
val opla=uap.sort(uap.col("avg(Rating)"));
display(opla);

// COMMAND ----------

// MAGIC %md App ratings per categpory are distributed between 4.0 and 4.5

// COMMAND ----------

// DBTITLE 1,Number of apps per category
val uap=df1.groupBy("Category").count();
val gasa=uap.sort(desc("count"))
display(gasa);

// COMMAND ----------

// DBTITLE 1,Observation
// MAGIC %md  Art_And_Design and Events category is having the least number of apps but they have the highest average rating.
// MAGIC 
// MAGIC Game,Family,Tools category apps have outnumbered the other apps

// COMMAND ----------

// MAGIC %md # Cleaned Columns-- Category,Rating,Reviews,Size  

// COMMAND ----------

// DBTITLE 1,Apps - Reviews
val pql=df1.sort(desc(("Reviews")));
pql.show(5)

// COMMAND ----------

// MAGIC %md Top 3 Apps having reviews greater than 60M-->['WhatsApp Messenger', 'Facebook', 'Instagram'] 

// COMMAND ----------

// DBTITLE 1,Distribution of Reviews Column
val pql=df1.select("Reviews");
display(pql);

// COMMAND ----------

// MAGIC %md # This data is highly skewed, which we can understand from the below box plot too.

// COMMAND ----------

val pql=df1.select("Reviews");
display(pql);

// COMMAND ----------

// DBTITLE 1,Suggestion
// MAGIC %md Log Transform

// COMMAND ----------

// DBTITLE 1,Cleaning Type Column
val opa=df1.withColumn("Type_cleaned",when(df1.col("Type")==="Free",0).otherwise(1));
val gsa=opa.select("Type_cleaned");
display(gsa)

// COMMAND ----------

// DBTITLE 1,Observation
// MAGIC %md Nearly 93% apps( 0.93*100=93) are Free and 7 % apps are paid

// COMMAND ----------

// DBTITLE 1,Save the Cleaned Type Column in original DataFrame & drop the previous one
val df1=opa.drop("Type");
df1.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Cleaning the Price Column
val flo=df1.withColumn("Price_Cleaned",when(df1.col("Price").isNull,0).otherwise(df1.col("Price")));
flo.show(100)

// COMMAND ----------

// DBTITLE 1,Convert the price of app from Dollar to Indian Rupees
val df1=flo.drop("Price").withColumn("Price_Rupees",flo.col("Price_Cleaned")*73.28).drop("Price_Cleaned");
df1.show()

// COMMAND ----------

// MAGIC %md # Cleaned Columns--Category,RatingReviews,Size,Type,Price

// COMMAND ----------

// DBTITLE 1,Correlation between price of the app and rating
println(df1.stat.corr("Rating","Price_Rupees"))

// COMMAND ----------

// MAGIC %md Yes, as the price increases ratings received seems to decrease in the appstore

// COMMAND ----------

// DBTITLE 1,App name with max price
val ps=df1.select(max(df1.col("Price_Rupees")));
val ms=ps.first()(0);
val dg=df1.filter(df1.col("Price_Rupees")===ms);
dg.show()

// COMMAND ----------

// MAGIC %md Maximum price is of : I'm Rich 

// COMMAND ----------

// DBTITLE 1,Cleaning of Installs Columns
df1.select("Installs").distinct().show()

// COMMAND ----------

// MAGIC %md We can simply convert Install column from String to Long (because of above data cleaning , "Free" value present in this column is removed)

// COMMAND ----------

// DBTITLE 1,Change the Datatype of Installs from String to Long
val df2=df1.withColumn("Installs_Long",df1.col("Installs").cast("Long"));
df2.show()

// COMMAND ----------

// DBTITLE 1,Drop the previous Installs column 
val df1=df2.drop("Installs").withColumnRenamed("Installs_Long","Installs");
df1.show()

// COMMAND ----------

val opa=df1.select("Installs");
display(opa)

// COMMAND ----------

// MAGIC %md #The plot is highly skewed and provides an insight from the descriptive statistics that there have been only few apps which has seen the maximum number of downloads .

// COMMAND ----------

// DBTITLE 1,Apps with Maximum number of installs
val ps=df1.select(max(df1.col("Installs")));
val ms=ps.first()(0);
val dg=df1.filter(df1.col("Installs")===ms);
dg.select("App").show()

// COMMAND ----------

// DBTITLE 1,What are the categories with most number of installs
val gs=df1.select("Category","Installs");
val ps=gs.groupBy("Category").sum("Installs");
val ls=ps.sort("sum(Installs)");
display(ls)

// COMMAND ----------

println(df1.stat.corr("Rating","Installs"))

// COMMAND ----------

// MAGIC %md # Obviously , number of install affect to rating

// COMMAND ----------

// DBTITLE 1,Check null values present in App Column
df1.filter(df1.col("App").isNull).count()

// COMMAND ----------

// DBTITLE 1,Removing the App which has null value
val df2=df1.filter(df1.col("App").isNotNull)

// COMMAND ----------

// DBTITLE 1,Updating the Original Dataframe
val df1=df2;

// COMMAND ----------

// DBTITLE 1,Calculate the length of the app names & store in a new column
val df2=df1.withColumn("Length_of_the_app_name",length(df1.col("App")));
df2.show()

// COMMAND ----------

// DBTITLE 1,Length of the App Name & Total number of Installation
val opls=df2.select("Installs","Length_of_the_app_name");
display(opls)

// COMMAND ----------

// DBTITLE 1,Correlation between Length of the App Name & Total number of Installation
opls.stat.corr("Installs","Length_of_the_app_name")

// COMMAND ----------

// MAGIC %md #It seems that most of the installs in the Play Store are contributed by apps having less than or equal to 4 words in their names. 
// MAGIC 
// MAGIC #Hence, it is better that developers use a concise word or words to name their app.

// COMMAND ----------

// DBTITLE 1,Check whether Null value is present in the Content_Rating Column
df1.filter(df1.col("Content_Rating").isNull).count()

// COMMAND ----------

// DBTITLE 1,Frequency of Distinct Content_Rating
val msp=df1.groupBy(df1.col("Content_Rating")).count();
val lps=msp.sort(desc("count"));
display(lps)

// COMMAND ----------

// DBTITLE 1,Inspection of Genres Column
// MAGIC %md A style or category of art, music, or literature

// COMMAND ----------

// DBTITLE 1,Check the distinct values
val og=df1.select("Genres").distinct();
og.show(og.count().toInt)

// COMMAND ----------

("Hello").length

// COMMAND ----------

// DBTITLE 1,Reducing the Genres into one simple category
def Genra_cleaning(x:String):String=
{
  var temp="";
  if(x.contains(";"))
  {
  var y=x.split(";");
  temp=y(0)
  }
  else
  {
    temp=x;
  }
  temp
}

// COMMAND ----------

// DBTITLE 1,Register the UDF
val udf_Genra_cleaning=udf(Genra_cleaning(_:String):String);

// COMMAND ----------

// DBTITLE 1,Apply the UDF to clean the column
val psr=df1.withColumn("Genre_Cleaned",udf_Genra_cleaning(df1.col("Genres")))
psr.show()

// COMMAND ----------

// DBTITLE 1,Drop the uncleaned Genres Column
val df1=psr.drop("Genres");

// COMMAND ----------

// DBTITLE 1,App count by Genres
val opla=df1.groupBy("Genre_Cleaned").count();
val gopla=opla.sort(desc("count"));
display(gopla)

// COMMAND ----------

// DBTITLE 1,Observation
// MAGIC %md "Tools" is the most common value with highest count(716) in Genres Column

// COMMAND ----------

// DBTITLE 1,Number of Installs from Individual Genres
val opla=df1.groupBy("Genre_Cleaned").sum("Installs");
val gopla=opla.sort(desc("sum(Installs)"));
display(gopla)

// COMMAND ----------


