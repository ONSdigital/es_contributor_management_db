# Contributor Management Database.
This is the repo for the code and SQL used to create and set up a postgres relational database in AWS using RDS and Terraform. The Contributor Management Database is to be used as the backend of the IBM alpha to produce a solution to the clerical resolution tasks needed to be performed by the business. The IBM alpha will produce a front end for the users of the results processing surveys to investigate and interrogate the data prior, during and after a survey run. This backend database will store the contributor data with contact info, queries or errors found and the auditting of the resolutions.

## Economic Stats-Terraform
Inital Azure Terraform script for Postgresql Database 

## Installing And Setting Up Terraform
1. To install Terraform, find the appropriate package for your system and download it. Terraform is packaged as a zip archive.

After downloading Terraform, unzip the package. Terraform runs as a single binary named terraform. Any other files in the package can be safely removed and Terraform will still function.

The final step is to make sure that the terraform binary is available on the PATH. See this page for instructions on setting the PATH on Linux and Mac. This page contains instructions for setting the PATH on Windows.

2. Get the required credentials from Azure:

* subscription_id       = 
* client_id             = 
* client_secret         = 
* tenant_id             = 


# Running Terraform
Run *terraform init* to import the different modules and set up remote state. When asked to provide a name for the state file choose the same name as the env value in your terraform.tfvars

Run *terraform plan* to check the output of terraform

Run *terraform apply* to create your infrastructure environment

Run *terraform destroy* to destroy your infrastructure environment
