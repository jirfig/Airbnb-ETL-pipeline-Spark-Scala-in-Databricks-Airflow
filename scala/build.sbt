name := "Airbnb-ETL"
version := "1.0"
scalaVersion := "2.12.10"

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % "provided"
libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.2.2" % "provided"
