# Google Cloud Project - Dataflow Pipeline
This Google Cloud Project contains a Dataflow Pipeline to read files in CSV from Google Cloud Storage to BigQuery and display dashboards with results in Tableau. The CSV files are the reports obtained from the [analysis of the "311 Service Requests" dataset](https://github.com/valentinajaramillor/bigdataprogram-applaudo).

## Folder and File Description
|      Element      | Description                                                                                                                                                                                                                                                            |
|:---------------------:|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|         data/         | It includes all the sub-folders corresponding to the reports that will be loaded into BigQuery. Each folder contains a ``data.csv`` file with the data and its ``schema.json`` with the schema structure.                                                          |
| data/report_names.txt | This text file lists the names of the subfolders that will be read by the script to generate an extract-transform-load job for each one, from the files ``data.csv`` and ``schema.json `` corresponding.                                                           |
|       src/       | Contains the python script [pipeline.py](.src/pipeline.py) that will generate the jobs in Dataflow to read the data from Cloud Storage and load it into BigQuery. |

To configure the environment and the requirements to run the pipeline, the following sections provide the details of each step.

## 1. Getting Started

1.1. Set the default project to your selected project for the deployment of the pipeline:

```shell
gcloud config set project <YOUR_PROJECT_ID>
```

1.2. Create a service account and provide it with the required permissions to use the Dataflow, Cloud Storage, BigQuery, etc:
```shell
gcloud iam service-accounts create <YOUR_SERVICE_ACCOUNT_NAME>

gcloud projects add-iam-policy-binding premium-student-369313 --member="serviceAccount:dataflowpipeline@premium-student-369313.iam.gserviceaccount.com" --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding premium-student-369313 --member="serviceAccount:dataflowpipeline@premium-student-369313.iam.gserviceaccount.com" --role=roles/storage.admin

gcloud projects add-iam-policy-binding premium-student-369313 --member="serviceAccount:dataflowpipeline@premium-student-369313.iam.gserviceaccount.com" --role=roles/dataflow.admin

gcloud projects add-iam-policy-binding premium-student-369313 --member="serviceAccount:dataflowpipeline@premium-student-369313.iam.gserviceaccount.com" --role=roles/dataflow.worker
```

Also, the role "Service Usage Admin" has to be provided also, it can be done through the Google Cloud Console, in IAM & Admin > IAM and editing the principal for the service account that is going to be used.

1.3. Create the keys.json to manage the credentials:
```shell
gcloud iam service-accounts keys create <YOUR_HOME_PATH>/keys.json --iam-account=<YOUR_SERVICE_ACCOUNT_NAME>@<YOUR_PROJECT_ID>.iam.gserviceaccount.com
```

1.4. Clone this repository to store it locally in the home path of the Google Cloud Terminal:
```shell
git clone https://github.com/valentinajaramillor/GCP_Tableau.git
cd GCP_Tableau/
```

### 2. Creating a New Cloud Storage Bucket
Then, it is required to create a new Cloud Storage Bucket to store the data folder with the report files in CSV format and their schemas, and also, to store the temporary files of the Dataflow jobs:

```shell
gsutil mb gs://<YOUR_BUCKET_NAME>
```

### 3. Copying Files to the Cloud Storage Bucket
Then you have to copy the data folder to the newly created bucket:

```shell
gsutil cp -r data/ gs://<YOUR_BUCKET_NAME>
```

### 4. Creating a BigQuery Dataset
To store the final tables with the reports, a BigQuery dataset has to be created:
```shell
bq mk <YOUR_DATASET_NAME>
```

## Building the Dataflow Pipeline
The Dataflow pipeline was built following the examples of the [GCP examples](https://github.com/GoogleCloudPlatform/professional-services/tree/main/examples/dataflow-python-examples/batch-examples/cookbook-examples), to ingest data from files (and their schemas) stored in Google Cloud Storage into Bigquery. The code was modified to support reading more than one file from GCS at once, and applying its respective schema, also stored in GCS, and finally creating one Dataflow job per report table.

The pipeline code splits the CSV files and transforms them to a format that can be read by BigQuery:
```python
class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    @staticmethod
    def parse_method(string_input, schema_json):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.
        Args:
            string_input: A comma separated list of values
            schema_json: The schema of the data in json format
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input.
         """

        # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',
                                                         string_input)))
        # Get the fields name from schema json
        field_names = tuple(field['name'] for field in schema_json['fields'])

        row = dict(
            zip(field_names,
                values))
        return row
```


## Execution of the Dataflow Jobs


### Set up the Python environment


### Running the Dataflow Pipeline


## Results and Dashboards


### Reports in BigQuery


### Dashboards in Tableau

