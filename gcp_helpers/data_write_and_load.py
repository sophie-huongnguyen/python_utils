from google.cloud import bigquery
import datetime
import pandas
import pytz

def bq_query_example() -> None:
    """
    Background setting
    - Credential json
    - Target dataset with query permission
    
    Step:
    - Construct a client
    - Make API query request through the client
    
    Query result methods
        - Check query variables
        - Make a dataframe
    """
    
    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = """
        SELECT name, SUM(number) as total_people
        FROM `bigquery-public-data.usa_names.usa_1910_2013`
        WHERE state = 'TX'
        GROUP BY name, state
        ORDER BY total_people DESC
        LIMIT 20
    """
    
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("name={}, count={}".format(row[0], row["total_people"]))
        
    output_df = query_job.to_dataframe() # Make a dataframe

def create_df_example() -> "pandas.core.frame.Pandas":
    """
    Create a data frame example
    Source https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
    """
    
    records = [
        {
            "title": "The Meaning of Life",
            "release_year": 1983,
            "length_minutes": 112.5,
            "release_date": pytz.timezone("Europe/Paris")
            .localize(datetime.datetime(1983, 5, 9, 13, 0, 0))
            .astimezone(pytz.utc),
            # Assume UTC timezone when a datetime object contains no timezone.
            "dvd_release": datetime.datetime(2002, 1, 22, 7, 0, 0),
        },
        {
            "title": "Monty Python and the Holy Grail",
            "release_year": 1975,
            "length_minutes": 91.5,
            "release_date": pytz.timezone("Europe/London")
            .localize(datetime.datetime(1975, 4, 9, 23, 59, 2))
            .astimezone(pytz.utc),
            "dvd_release": datetime.datetime(2002, 7, 16, 9, 0, 0),
        },
        {
            "title": "Life of Brian",
            "release_year": 1979,
            "length_minutes": 94.25,
            "release_date": pytz.timezone("America/New_York")
            .localize(datetime.datetime(1979, 8, 17, 23, 59, 5))
            .astimezone(pytz.utc),
            "dvd_release": datetime.datetime(2008, 1, 14, 8, 0, 0),
        },
        {
            "title": "And Now for Something Completely Different",
            "release_year": 1971,
            "length_minutes": 88.0,
            "release_date": pytz.timezone("Europe/London")
            .localize(datetime.datetime(1971, 9, 28, 23, 59, 7))
            .astimezone(pytz.utc),
            "dvd_release": datetime.datetime(2003, 10, 22, 10, 0, 0),
        },
    ]

    dataframe = pandas.DataFrame(
        records,
        # In the loaded table, the column order reflects the order of the
        # columns in the DataFrame.
        columns=[
            "title",
            "release_year",
            "length_minutes",
            "release_date",
            "dvd_release",
        ],
        # Optionally, set a named index, which can also be written to the
        # BigQuery table.
        index=pandas.Index(
            ["Q24980", "Q25043", "Q24953", "Q16403"], name="wikidata_id"
        ),
    )
    
    return dataframe

def bq_write_example() -> None:
    """
    Source: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
    Background setting
    - Credential json
    - Destination table_id with write permission
    
    Step:
    - Gather a data frame
    - Make API query request through the client
    - Set up a load-job configuration 
    https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJobConfig
        - Schema 
            - SchemaField type 
            https://cloud.google.com/python/docs/reference/bigquery/latest/enums#class-googlecloudbigqueryenumssqltypenamesvalue
        - write_disposition: strings of three values
            - WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the data, 
            removes the constraints, and uses the schema from the query result.
            - WRITE_APPEND: If the table already exists, BigQuery appends the data to the table.
            - WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned in the job result.

    
    Query result methods
        - Check query variables
        - Make a dataframe
    """
    
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "your-project.your_dataset.your_table_name"

    dataframe = create_df_example()

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    client = bigquery.Client() # Construct a BigQuery client object.
    
    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )    