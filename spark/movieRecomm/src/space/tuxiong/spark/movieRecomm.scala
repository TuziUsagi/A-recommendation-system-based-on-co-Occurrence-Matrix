package space.tuxiong.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.nio.charset.CodingErrorAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.io.{Codec, Source}
import scala.collection.mutable.Map

object movieRecomm {
  
  private val scMode: String = "local[*]"
  private val userData: String = "../ml-100k/u.data"
  private val userHistoryFileName: String = "../ml-100k/u.hist"
  private val movieDB: String = "../ml-100k/u.item"
  // Create a SparkContext
  val spark = new SparkContext(scMode, "movieRecomm")

  // Read user ratings from file
  private def mapUserIdAndMovieID(): RDD[(Int,Int)] = 
  {
    val dataFile: RDD[String] =spark.textFile(userData)
    val userIdMappedWithMovieIdAndRating: RDD[(Int, Int)] = 
      dataFile.map(line =>
        {
          val fields=line.split("\\s+") //Using regex to split
          (fields(0).toInt, fields(1).toInt) // userID->movieID
        })
    return userIdMappedWithMovieIdAndRating
  }
  
  def mapMovieIdAndName():Map[Int,String] = 
  {
  // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val IDwithName: Map[Int, String] = collection.mutable.Map[Int, String]()
    for(line <- Source.fromFile(movieDB).getLines)
    {
      val fields=line.split('|')
      IDwithName.put(fields(0).toInt, fields(1))
    }
    return IDwithName
  }
  
  // Read a single user's history for recommendation
  private def loadUserHistory(): Map[Int, Double] = 
  {
    val userHistoryMap: Map[Int, Double] = collection.mutable.Map[Int, Double]()
    for (line <- Source.fromFile(userHistoryFileName).getLines) 
    {
      val fields=line.split("\\s+")
      userHistoryMap.put(fields(0).toInt, fields(1).toDouble)
    }
    return userHistoryMap
  }
  
  private def multiplyMatByVec(x: ((Int, Int), Double), y:Map[Int, Double]): ((Int,Int), Double) = 
  {
    val curMovieID = x._1._2
    return (x._1, x._2 * y.get(curMovieID).getOrElse(0.0))
  }
  
  private def getNorm(x:(Int, (Double, Double))): (Int, Double) = 
  {
    if(x._2._2 < 1) (x._1, x._2._1)
    else  (x._1, x._2._1/x._2._2)
  }
  
  private def filterMovie(x: (Int, Double), y:Map[Int, Double]): Boolean =
  {
    if(y.get(x._1) != None) false
    else true
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) 
  {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Get UserID->MovieID pair
    val userIDtoMovieID: RDD[(Int, Int)] = mapUserIdAndMovieID()
    // Get UserID->(MovieID, MovieID), every two movies watched by each user
    val userIDtoMoviePair: RDD[(Int, (Int, Int))] = userIDtoMovieID.join(userIDtoMovieID)
    // (MovieID, MovieID) -> 1
    val moviePair: RDD[((Int, Int), Double)] = userIDtoMoviePair.map(x => (x._2, 1.0))
    // co-occurrence matrix for each movie pairs
    val movieIDcoMat: RDD[((Int, Int), Double)] = moviePair.reduceByKey((x, y) => x+y)
    // Get the normalization factor for each row
    val coMatRow: RDD[(Int, Double)] = movieIDcoMat.map(x => (x._1._1, x._2))
    val coMatRowNormFactor: RDD[(Int, Double)] = coMatRow.reduceByKey((x:Double, y:Double) => x+y)
    // Get user viewing history, broadcast them since history of single user is usually small.
    val userHistoryMap = spark.broadcast(loadUserHistory())
    // Matrix multiple step 1
    val matStep1 = movieIDcoMat.map(x => multiplyMatByVec(x, userHistoryMap.value))
    // Matrix multiple step 2, add each row
    val matStep2: RDD[(Int, Double)] = matStep1.map(x => (x._1._1, x._2)).reduceByKey((x:Double, y:Double) => x+y)
    // Normalization
    val recommFactorWithNorm: RDD[(Int, (Double, Double))] = matStep2.join(coMatRowNormFactor)
    val recommFactorNorm: RDD[(Int, Double)] = recommFactorWithNorm.map(x => getNorm(x))
    //Remove the movies that the user has already watched
    val recommendedMovie = recommFactorNorm.filter(x => filterMovie(x, userHistoryMap.value))
    //Get movie ID and the corresponding movie title
    val movieNameMap: Map[Int, String] = mapMovieIdAndName()
    //Switch key and value (recommendation score) then sort by value then take first ten results to print
    recommendedMovie.map(x => (x._2, x._1)).sortByKey(false).take(10).map(x => println(movieNameMap.get(x._2).getOrElse("")))
  }
}