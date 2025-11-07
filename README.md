# Databricks_Medallion_Architecture
A demonstration of the Medallion Architecture in Databricks using databricks_simulated_retail_customer_data from Databricks Marketplace. Includes Pyspark notebooks, SQL file for LDP build, config YAML files for jobs, config JSON file for Dashboard, and instructions for AI Genie space.

## Loading Data
Data is available on Databrick Marketplace. Search for **Simulated Retail Customer Data** on Databricks Marketplace and click "Get Instant Access". The bronze table code assumes loading in files from the delta share of this data from Databricks Marketplace.

Next, run the Create Sim Retail Demo Schema file within the Config folder to create the schema and volumes for this demo. Be sure to configure the catalog parameter before running the notebook to control where you want this demo data to live (default is workspace).

To load data from the delta share into your volumes, use the Python functions in Sim Retail Data Notebooks > Add Files. Recommended to call the `add_next_file()` function to incrementally load files one by one, simulating batch updated data. Be sure to configure the catalog parameter before running in notebook. **Also make sure to update the desired catalog in the `add_next_file.py` file** (you can run this python script to add a file and it will be used later in jobs to add files before kicking off pipelines). Make sure to run this at least once before running any Pyspark or SQL code to build / update tables.

## Build Pyspark Tables
The Pyspark Bronze/Silver/Gold notebooks in the Sim Retail Data Notebooks folder use Pyspark to build the bronze, silver, and gold layers of the demo. Once a file has been added to the customer, orders, and status volumes you can run these notebooks to create the tables in the `sim_retail_demo` schema.

## Build SQL LDP (Lakeflow Declarative Pipeline)
[Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/) is a framework for creating batch and streaming data pipelines in SQL and Python. The declarative SQL file to built the pipeline is in the Sim Retail LPD > dlt_pipeline.sql file. To build the pipeline, you simply need to right click the Sim Retail LDP folder and follow the menu create > ELT Pipeline. You can also create the pipeline in the Jobs & Pipelines tab on the left hand menu. Specifically name the pipeline **Customer - Order Pipeline (SQL LDP)**. 

**Before running, make sure to add in configuration parameters to the pipeline under settings.** You can also see the YAML configuration file under the Config folder. It is not recommended to paste these settings into your pipeline unless you are comfortable manually updating the root path and libraries yourself.

## Dashboard
Under the Config folder, there is a JSON file containing the Customer and Orders dashboard configuration. If you are using a different catalog than `workspace`, download this JSON file and run a replace all on "workspace" for your catalog name. Then in the Dashboards tab, click the dropdown by "Create dashboard" and select _Import dashboard from file_. This will then fully build the dashboard in your workspace using the `_pyspark` tables built from the notebooks above.

## Jobs
The configuration files for the jobs in this demo can be found in the Config folder. Creating strictly from these YAML files might not work because of specific job and pipeline IDs in the file, as well as different file paths. It is recommended to build the jobs yourself and reference these YAML files to check for correct configuration.

### Customer - Order Pipeline (SQL LDP)
This is an ELT pipeline created with delcarative SQL. See above.

### Customer - Orders Pipeline (Pyspark Job)
This job sequentially runs the Pyspark Bronze/Silver/Gold Notebooks to build the Pyspark tables in the medallion architecture. Add tasks that run these notebooks, starting with bronze then adding silver and gold with the right dependencies. Make sure to add job parameters for `catalog` and `schema` matching where the demo data is stored in your workspace.

### Master Customer - Orders Job
This is the main job to run when conducting this demo. The first task is to run the `add_next_file.py` file to load a new volume, simulating new data. Next add a task to run the Customer - Orders Pipeline (SQL LDP) pipeline and the Customer - Orders Pipeline (Pyspark Job) sequentially, with both dependent on Add_Next_File. Finally, add in a Dashboard Refresh of the Customer and Orders dashboard. For the demo, it's cool to configure yourself as a subscriber so you can show how the update notification works.

## AI Genie
There is currently no default JSON or YAML configuration file for Genie spaces. Instead, some of the configuration elements are listed in the Customer - Orders Retail Genie Config notebook under the Config folder. This includes the settings, instructions, and included data for the demo Genie space. Also included are good questions to ask Genie during a live demo. It is recommended that you experiment and tinker with Genie settings to understand the tool and refine for live demos.

