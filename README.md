## Project Summary
### Implement Data Warehouse for Data Engineering nanodegree program from Udacity.
#### Objective:
The purpose of this project is to implement a Cloud Data Warehouse using the following technologies:
* Amazon Redshift
* Python 3.6+
* psycopg2 library
* configparser library

In order to install libraries and dependencies, use this [link](https://pypi.org/project/psycopg2/).

## Business Context
A music streaming company, named Sparkify, has files in json format stored in an Amazon S3 Bucket.
It is needed to put their data on Amazon Redshift which is the cloud Data Warehouse service provided by Amazon AWS.

## How did we tackle the challenge?
In order to do so, we modeled temporary tables (named staging tables as well) and load the data from json files
into them.
Then, we modeled a star schema as follows:
* Fact table:
    * **songplays** - records in event data associated with song plays i.e. records with page NextSong. 
        * ***songplay_id***, 
        * ***start_time***, 
        * ***user_id, level***, 
        * ***song_id***, 
        * ***artist_id***, 
        * ***session_id***, 
        * ***location***, 
        * ***user_agent***
    
* Dimension tables:
    * **users** - users in the app:
        * ***user_id***,
        * ***first_name***,
        * ***last_name***,
        * ***gender***,
        * ***level***
    * **songs** - songs in the music database:
        * ***song_id***,
        * ***title***,
        * ***artist_id***,
        * ***year***,
        * ***duration***
    * **artists** - artists in music database:
        * ***artist_id***,
        * ***name***,
        * ***location***,
        * ***latitude***,
        * ***longitude***
    * **time** - timestamps of records in **songplays** broken down into specific units:
        * ***start_time***,
        * ***hour***,
        * ***day***,
        * ***week***,
        * ***month***,
        * ***year***,
        * ***weekday***
    
## Project Files
* ### create_table.py
    - Creates fact and dimension tables described above in the previous topic according to the modeled star schema.
    
* ### etl.py
    - Loads data from S3 into staging tables on Redshift and then process the data into final analytics tables.
    
* ### sql_queries.py
    - Defines our SQL statements. It is imported into the two files above.
    
* ### dwh.cfg
    - Contains information about the public S3 Bucket which will be accessed.
    - You must fill the information about the cluster, and the IAM role you must create.
    
## Running the project
### Follow the steps below in order to run the project:
    1. Make sure you created an Amazon Redshift Cluster (same region as your bucket), an IAM role, and attached to it the right access policy.
    2. Run the create_tables.py file
    3. Run the etl.py file.

       
    
