{
  try {
    import spark.implicits._
    println("SPARK_MASTER=" + spark.sparkContext.master)
    println("SPARK_VERSION=" + spark.version)
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    val visits = spark.read.textFile(sys.env("BENCH_INPUT")).map { line =>
      val a = line.split("\t", -1)
      require(a.length == 4 && a.forall(_.nonEmpty), "Invalid benchmark row")
      require(!line.contains("\r"), "Invalid benchmark row")
      require(a(1).matches("[0-9]{4}") && a(2).matches("[1-9]|1[0-2]"),
              "Invalid benchmark row")
      (a(0), a(1).toInt, a(2).toInt, a(3))
    }.toDF("clinic", "visit_year", "visit_month", "visit_type")
    visits.createOrReplaceTempView("benchmark_visits")
    val result = spark.sql("""
      SELECT clinic, visit_year, visit_month, visit_type, COUNT(*) AS visit_count
      FROM benchmark_visits
      GROUP BY clinic, visit_year, visit_month, visit_type
    """)
    result.map(r => r.getString(0) + "\t" + r.getInt(1) + "\t" +
      r.getInt(2) + "\t" + r.getString(3) + "\t" + r.getLong(4))
      .write.mode("error").text(sys.env("BENCH_OUTPUT"))
    println("ASHRAFUR_SPARK_PASS")
    spark.stop(); System.exit(0)
  } catch { case e: Throwable => e.printStackTrace(); System.exit(1) }
}
