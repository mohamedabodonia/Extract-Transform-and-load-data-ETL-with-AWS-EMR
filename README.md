# Extract-Transform-and-load-data-ETL-with-AWS-EMR
Extract data from s3 make some transformation and load in another s3
## overview

This project applied on data for a market.  
In this project, i worked on creating data pipline using aws services (aws EMR).   
I downloaded data from internet website then apply ETL pipline proecss on data. 

## project workflow

**1-Data Acquisition**: Downloaded the Spotify data file from Kaggle.

**2-AWS S3 Setup**: Created two S3 buckets on AWS Console  
* one as the source for the row data
* anther as the destination for the transformed data.  

**3-Data Processing with AWS EMR**: applying some function on file   
* drop NULLs values.
* extract the year and the monthe of the "period_end" coulmn.

**4-Data Storage**: Save the processed data in the destination S3 bucket.

## Technology used 

* **s3 bucket**: used as a data storagae.  
* **aws EMR** : used as a Big Data tool to apply transformation n huge amount of data.
* **aws VPC** : Create a secure, private network within AWS, isolated from other networks.
* **aws IAM** : to give temporary access rights for AWS services or external entities.
* **aws keypair** :to enable ssh.   

## Work Flow & Arcitecture
* Save the row data in s3 "store-row-data-yml"
* Use Aws EMR as a big data tool (spark) and create jupiter note book to write a code of Pyspark to do transformation on data.
* Save transformed data on s3 bucket "redfine-transformed-yml"
  
<img src="https://github.com/mohamedabodonia/Extract-Transform-and-load-data-ETL-with-AWS-EMR/blob/main/architecture.jpeg?raw=true">

## Configure Resources on AWS Concole
**1- configure data Storage**  
create 3  buckets of s3.    
firs for the Row data "store-row-data-yml".  
second for transformed data "redfine-transformed-yml".   
third for the logs of EMR "AWS logs.  
fourth for the studio workspace for jupiter notebook"EMR-studio-bucket". 

<img src="https://github.com/mohamedabodonia/Extract-Transform-and-load-data-ETL-with-AWS-EMR/blob/main/s3%20bucket.jpeg?raw=true">


**2- configure VPC**

<img src="https://github.com/mohamedabodonia/Extract-Transform-and-load-data-ETL-with-AWS-EMR/blob/main/VPC.jpeg?raw=true">


**3- configure AWS EMR**

create master node and worker node.  
determine the minmum and maximum number of cluster.  



<img src="https://github.com/mohamedabodonia/Extract-Transform-and-load-data-ETL-with-AWS-EMR/blob/main/EMR.jpeg?raw=true">


**4- create EMR studio**


<img src="https://github.com/mohamedabodonia/Extract-Transform-and-load-data-ETL-with-AWS-EMR/blob/main/EMR%20studio.jpeg?raw=true">


* Then, create workspace to create Jupiter noteBook

<img src="https://github.com/mohamedabodonia/Extract-Transform-and-load-data-ETL-with-AWS-EMR/blob/main/workspace.jpeg?raw=true">

* Jupiter notebook:enable to write a py spark code

<img src="https://github.com/mohamedabodonia/Extract-Transform-and-load-data-ETL-with-AWS-EMR/blob/main/jupiter%20notebook.jpeg?raw=true">




