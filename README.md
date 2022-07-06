
#### How to run

This code was developed using a local deployment of airflow because I had some issues with GCP that blocked me from continuing development there.

In order to run locally you will need to install the below packages using Pip:

- apache-airflow-providers-google
- cryptography

after you've installed the relevant packages you will need to create a service account to access GCS and Big Query.

These are the roles I have for the service account I created in my GCP account:

- BigQuery Data Editor
- BigQuery Job User
- Storage Object Admin

Then you will want to create the following resources:

- a GCS bucket named:           `user-data-etl-demo`
- a BQ dataset named:           `syrinx`
- a table in the dataset named: `users`

If you cannot get it running or would rather me just demo the code for you I am happy to do it for you over a video call!

Please let me know if you have any questions. My email is noah@noahgary.io 🙂
