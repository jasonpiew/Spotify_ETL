# Spotify_ETL
Welcome to my Project :D.

in this repository we will Learn how to extract and model a spotify project using mainly dbt.

There will be some step that we will undergo such as:
1. Getting a request from Spotify to fetch our data, in this case I download all my json data from spotify, You have to set up an spotify account (obviously) then on the website > go https://www.spotify.com/ca-en/account/privacy/ > scroll down below and pick request data. You will have to wait for up to 2 weeks. After all is done, download and store your json.
2. We will use python to read through all the json and append it into a dataframe, and then we connect it into our database, which I use Postgres.
3. After we store our data, we will use dbt to transform and model.
4. We will visualize our transformed data with Metabase

## Prerequisite:
1. Python
2. Docker (to host PostgresQL server and Metabase)
3. Dbeaver (I use this as a database IDE)
4. dbt (I use dbt CLI)

### Folder Organization:
1. Python > stores the script to read data and send it into our database
2. Transform > stores the structure of dbt

#### Note
Please do remember you will need to host your Docker, and the code is not for practical solution. But to give you a glimpse of how this project works. Feel free to contact me on LinkedIn if you'd like to discuss : https://www.linkedin.com/in/jason-piew/! see ya there
