package spark.rddtodataset

// user id | item id | rating | timestamp.
case class MLRating(userId: Integer, itemId: Integer, rating: Double, timestamp: Long)
