import org.apache.spark.sql.SparkSession

// Comment twitch a évolué de 2016 à 2021 ?
object Twich {
  val TWITCH_GAME_PATH  = "C:\\Users\\Toky Cedric\\Desktop\\Etudes\\4 eme année\\Spark\\Twich_Project\\Twitch_game_data.csv"
  val TWITCH_GLOBAL_PATH = "C:\\Users\\Toky Cedric\\Desktop\\Etudes\\4 eme année\\Spark\\Twich_Project\\Twitch_global_data.csv"
  def main(args : Array[String]) : Unit = {

    // Initialisation du sparkSession
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TwichProject")
      .getOrCreate()

    // Importation du Dataset des Jeux sur Twitch
    val twitch_game = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TWITCH_GAME_PATH)

    // Importation du Dataset global Twitch
    val twitch_global = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TWITCH_GLOBAL_PATH)

    // Affiche le Schema du Dataset
    twitch_game.printSchema()
    twitch_global.printSchema()

    // Afiche les 20 premières lignes du Dataset
    twitch_game.show(20)
    twitch_global.show(20)



  }
}
