# ARIS DAGS for Airflow


## Summary

|  |  |
|---|---|
| DSAA Team Lead | Mike Trinh | 
| Project Tier | Tear 3 | 
| DSAA Team Members | Graham Chickering |
| Client(s) | ARIS  |
| Internal Client(s) |  |
| Project Start Date | 01/01/2022 |
| Project End Date | 12/12/2024 |
| Status | In progress (testing out pipeline in first year of development |                                                                 

 ## Summary

This repo contains all the scripts needed to run different parts of the ARIS pipeline. This repo is connected to multiple remote servers that help actually perform all the steps within the pipeline process. These steps include webscraping data from Department of Education website, running sas scripts to transform data into useable format, running quality checks, uploading the transformed data to a MySQL database, and ultimately creating different digest tables

To run these scripts login to the ARIS Airflow server found here.
https://aris-airflow.uat.air.org/login/?next=https%3A%2F%2Faris-airflow.uat.air.org%2Fhome



Any additional information goes here. Could be related projects, helpful links to packages used, etc.
- If any of the scripts that the pipeline calls need to be switched, these can be found in the github repo, https://github.com/American-Institutes-for-Research/dsaa_aris_autoDigest. 
