import com.mashape.unirest.http.Unirest
import org.apache.spark.sql.SparkSession
import com.typesafe.config._
import resources._

object footballCenter {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()

    val response = Unirest.get("https://api.football-data.org/v2/matches?dateFrom=2020-06-29&dateTo=2020-06-29")
      .header("x-auth-token", conf.getString("api_key"))
      .asJson().getBody().getObject().get("matches").toString()

    val spark = SparkSession.builder()
      .appName("footballCenter")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    val df = spark.read.json(Seq(response).toDS)
    df.selectExpr("competition.name as competition", "competition.area.name as country_name", "group",
      "homeTeam.name as home_team", "score.fullTime.homeTeam as home_team_score",
      "awayTeam.name as away_team", "score.fullTime.awayTeam as away_team_score",
      "case when score.winner = 'AWAY_TEAM' then awayTeam.name else homeTeam.name end as winner").show()
  }
}
