package spark.process

// user id | item id | rating | timestamp.
case class RatingML(userId: Integer, movieId: Integer, rating: Double, timestamp: Long)
