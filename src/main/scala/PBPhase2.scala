import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark.{SparkConf, SparkContext}

// For implicit conversions from RDDs to DataFrames

object PBPhase2 {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // Contains SQLContext which is necessary to execute SQL queries
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Reads json file and stores in a variable
    val tweet = sqlContext.read.json("C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")

    //To register tweets data as a table
    tweet.createOrReplaceTempView("tweets")

    println("Enter any one of the following query to get data")
    println("1.Query-1:Users who created accounts on March and May  ")
    println("2.Query-2:Users created where user name is not null")
    println("3.Query-3:Number of  tweets on accounts")
    println("4.Query-4:Tweets from differnt countries")
    println("5.Query-5:Number of users created according to day wise")
    println("6.Query-6:Normal users(not verified accounts) with more number of followers")
    println("7.Query-7:Number of tweets based of different states in USA")
    println("8.Query-8:Account verification Tweets")
    println("9.Query-9:Top Tweet text and Retweet count")
    println("10.Query-10:Highest tweeted tweets")
    println("11.Query-11:Users tweeted based on days")
    println("Enter any one of the following query to get data:")
    val count = scala.io.StdIn.readLine()
    count match {
      case "1" =>
        val df = sqlContext.read.json(
          "C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")
        df.createOrReplaceTempView("tweets")
        val query = sqlContext.sql(
          ("SELECT substring( user.created_at, 5, 3) as month, count(user.id) as count from tweets group by month"))
        println("************************************************")
        println("Users with accounts created using specified month")
        println("************************************************")
        query.show()

      /*-----------------------------Query 2:  Users created where user name is not null--------------------------------------------*/
      case "2" =>

        val query = sqlContext.sql("SELECT count(*) as count,user.name from tweets where user.name is not null group by user.name order by count desc limit 10")


        println("****************************************")
        println("Users created where user name is not null")
        println("****************************************")
        query.show()

      //TopTweets.collect().foreach(println)
      //TopTweets.write.format("com.databricks.spark.csv").option("header", "true").save("C:\\Users\\nikky\\Desktop\\pbproject\\TopTweetsBySports.csv")

      /*-----------------------------------Query 3: Number of  tweets on accounts-------------------------------------*/
      case "3" =>
        val df = sqlContext.read.json(
          "C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")
        df.createOrReplaceTempView("tweets")
        val query3 = sqlContext.sql(
          ("select count(*) as count,q.text from (select case when text like '%coronavirus%' then 'coronavirus' when text like '%Narendra Modi%' then 'Narendra Modi' " +
            "when text like '%realDonaldTrump%' then 'realDonaldTrump'" +
            "when text like '%BTS_twt%' then 'BTS_twt' when text like '%Oscars%' then 'Oscars' when text like '%Grammy%' then 'Grammy'when text like '%iphone%' " +
            "then 'iphone'when text like '%tesla%' then 'tesla' else 'different sports' end as text from tweets)q group by q.text"))

            println("*****************************************")
        println("Number of  tweets on accounts")
        println("*****************************************")
        query3.show()

      /*-------------------------------Query 4 : Tweets from differnt countries-----------------------------------*/
      case "4" =>
      val hf = sqlContext.read.json(
    "C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")
    hf.createOrReplaceTempView("tweets")
    val query = sqlContext.sql(
    "SELECT place.country,count(*) AS count FROM tweets GROUP BY place.country ORDER BY count DESC")
    println("************************************************")
    println("Tweets from differnt countries")
    println("************************************************")
    query.show()

      /*-------------------------------Query 5 :Number of users created according to day wise -----------------------------------*/
      case "5" =>
        val hf = sqlContext.read.json(
          "C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")
        hf.createOrReplaceTempView("tweets")
        val query = sqlContext.sql(
          "SELECT substring(user.created_at, 1, 3) as day,count(*) as count from tweets group by day")
        println("************************************************")
        println("Number of users created according to day wise")
        println("************************************************")
        query.show()

      /*-----------------------Query 6: Normal users(not verified accounts) with more number of followers----------------------------------*/
      case "6" =>
        val df = sqlContext.read.json(
          "C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")
        df.createOrReplaceTempView("tweets")
        val query = sqlContext.sql(
          "SELECT user.verified,user.screen_name,user.followers_count FROM tweets WHERE user.verified = false ORDER BY  user.followers_count DESC LIMIT 20")
        println("************************************************")
        println("Normal users(not verified accounts) with more number of followers")
        println("************************************************")
        query.show()

      /*---------------------Query 7: Number of tweets based of different states in USA--------------------------*/
      case "7" =>
        val df = sqlContext.read.json(
          "C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")
        df.createOrReplaceTempView("tweets")
        val location = sqlContext.sql("SELECT user.location,count(text) as count FROM tweets WHERE place.country='United States' AND user.location is not null GROUP BY user.location ORDER BY count DESC")
        location.createOrReplaceTempView("location")
        location.show()

      /*-----------------------------Query 8: Account verification Tweets-----------------------------------------------*/
      case "8" =>
        val acctVerify=sqlContext.sql("SELECT distinct id, " +
          "CASE when user.verified LIKE '%true%' THEN 'VERIFIED ACCOUNT'"+
          "when user.verified LIKE '%false%' THEN 'NON-VERIFIED ACCOUNT'"+
          "END AS Verified from tweets where text is not null")
        acctVerify.createOrReplaceTempView("acctVerify")
        var acctVerifydata=sqlContext.sql("SELECT  Verified, Count(Verified) as Count from acctVerify where id is NOT NULL and Verified is not null group by Verified order by Count DESC")

        println("************************************************")
        println("Account verification Tweets")
        println("************************************************")
        acctVerifydata.show()

      /*----------------------------Query 9: Top Tweet text and Retweet count----------------------------------------*/
      case "9" =>
        val SQLquery = sqlContext.sql("SELECT user.name as Name,retweeted_status.text AS Retweet_Text,retweeted_status.retweet_count AS Retweet_Count FROM tweets WHERE retweeted_status.retweet_count IS NOT NULL ORDER BY retweeted_status.retweet_count DESC")

        println("************************************************")
        println("Top Tweet text and Retweet count")
        println("************************************************")
        SQLquery.show()

      /*----------------------------Query 10: Users created per year----------------------------------------*/
      case "10" =>
        val df = sqlContext.read.json(
          "C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")
        df.createOrReplaceTempView("tweets")
        val SQLquery = sqlContext.sql("SELECT user.screen_name,text,retweeted_status.retweet_count FROM tweets ORDER BY retweeted_status.retweet_count DESC LIMIT 20")

        println("************************************************")
        println("Highest retweeted Tweets ")
        println("************************************************")
        SQLquery.show()
      /*----------------------------Query 11: Users tweeted based on days----------------------------------------*/
      case "11" =>
        val df = sqlContext.read.json(
          "C:\\Users\\jhima\\Desktop\\pb\\tweetsdata.json")
        df.createOrReplaceTempView("tweets")
        val SQLquery = sqlContext.sql("SELECT substring(quoted_status.created_at,1,3) as day,count(text) as count FROM tweets GROUP BY day")

        println("************************************************")
        println("Users tweeted based on days ")
        println("************************************************")
        SQLquery.show()
        ;
    }
  }
}