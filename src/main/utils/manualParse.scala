import org.json.JSONArray
import org.apache.spark.sql.Row

object manualParse {
  val ja = new JSONArray(response)
  val objects = (0 until ja.length()).map(ja.getJSONObject)
      val seq = scala.collection.mutable.Buffer[Row]()
      for (item <- objects) {
        val row = Row(
          item.getJSONObject("competition").get("name"),
          item.getJSONObject("competition").getJSONObject("area").get("name"),
          item.get("group"),
          item.getJSONObject("homeTeam").get("name"),
          item.getJSONObject("score").getJSONObject("fullTime").get("homeTeam"),
          item.getJSONObject("awayTeam").get("name"),
          item.getJSONObject("score").getJSONObject("fullTime").get("awayTeam"),
          item.getJSONObject("score").get("winner")
        )
        seq += row
      }
}
