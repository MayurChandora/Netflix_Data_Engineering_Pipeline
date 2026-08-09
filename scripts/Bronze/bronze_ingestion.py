from pyspark import pipelines as dp

# BRONZE - NETFLIX TITLES
@dp.table(
    name="titles",
    comment="Raw incremental ingestion of Netflix titles"
)
def titles():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header","true")
            .option("pathGlobFilter", "netflix_titles.csv")
            .load("/Volumes/netflix_catalog/bronze/landing/batch_001/")
    )


# BRONZE - NETFLIX CAST
@dp.table(
    name="cast",
    comment="Raw incremental ingestion of Netflix cast"
)
def cast():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "netflix_cast.csv")
        .load(
            "/Volumes/netflix_catalog/bronze/landing/batch_001/"
        )
    )



# BRONZE - NETFLIX CATEGORY
@dp.table(
    name="category",
    comment="Raw incremental ingestion of Netflix categories"
)
def category():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "netflix_category.csv")
        .load(
            "/Volumes/netflix_catalog/bronze/landing/batch_001/"
        )
    )



# BRONZE - NETFLIX COUNTRIES
@dp.table(
    name="countries",
    comment="Raw incremental ingestion of Netflix countries"
)
def countries():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "netflix_countries.csv")
        .load(
            "/Volumes/netflix_catalog/bronze/landing/batch_001/"
        )
    )


# BRONZE - NETFLIX DIRECTORS
@dp.table(
    name="directors",
    comment="Raw incremental ingestion of Netflix directors"
)
def directors():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "netflix_directors.csv")
        .load(
            "/Volumes/netflix_catalog/bronze/landing/batch_001/"
        )
    )