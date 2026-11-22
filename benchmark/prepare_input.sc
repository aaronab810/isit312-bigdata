{
  try {
    import spark.implicits._
    val visits = spark.read.parquet(sys.env("ACCEPTED_VISITS"))
    visits.createOrReplaceTempView("accepted_visits")
    visits.printSchema()
    require(spark.sql("""
      SELECT * FROM accepted_visits
      WHERE clinic IS NULL OR trim(clinic) = ''
         OR clinic_patient_id IS NULL OR trim(clinic_patient_id) = ''
         OR visit_id IS NULL OR trim(visit_id) = ''
         OR visit_date IS NULL OR visit_type IS NULL OR trim(visit_type) = ''
    """).count() == 0, "Missing accepted visit fields")
    require(spark.sql("""
      SELECT clinic, clinic_patient_id, visit_id FROM accepted_visits
      GROUP BY clinic, clinic_patient_id, visit_id HAVING COUNT(*) <> 1
    """).count() == 0, "Duplicate visit keys")
    val n = visits.count()
    require(n == 61527L, "Expected 61527 baseline visits, got " + n)
    val sample = spark.sql("""
      SELECT COUNT(*) FROM accepted_visits WHERE clinic = 'SITE-08'
      AND year(visit_date) = 2024 AND month(visit_date) = 7
      AND visit_type = 'procedure'
    """).first().getLong(0)
    require(sample == 20L, "Expected demo group count 20")
    val normalized = spark.sql("""
      SELECT clinic, year(visit_date) AS visit_year,
             month(visit_date) AS visit_month, visit_type
      FROM accepted_visits
    """).map { r =>
      val clinic = r.getString(0)
      val kind = r.getString(3)
      require(!clinic.exists(c => c == '\t' || c == '\r' || c == '\n') &&
              !kind.exists(c => c == '\t' || c == '\r' || c == '\n'),
              "Unsafe delimiter in grouping field")
      clinic + "\t" + r.getInt(1) + "\t" + r.getInt(2) + "\t" + kind
    }.toDF("value")
    normalized.write.mode("error").text(sys.env("BENCH_ROOT") + "/input/original")
    normalized.crossJoin(spark.range(10).toDF("copy_number")).select("value")
      .write.mode("error").text(sys.env("BENCH_ROOT") + "/input/larger")
    println("ORIGINAL_ROWS=" + n)
    println("LARGER_ROWS=" + (n * 10))
    println("DEMO_COUNT=" + sample)
    println("ASHRAFUR_PREPARE_PASS")
    spark.stop(); System.exit(0)
  } catch { case e: Throwable => e.printStackTrace(); System.exit(1) }
}
