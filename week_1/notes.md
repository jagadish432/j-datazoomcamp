# Useful commands

## ssh
1. ssh -i ~/.ssh/gcp jagadish@public_ip

2. ssh de-zoomcamp (we need to put the alias in our ~/.ssh/config file in our personal laptop/machine)


## use vscode 
1. to connect to remote using the alias we created
2. we can forward the ports from remote machine to the local machine


## Docker useful notes
1. https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md

2. newgrp docker(if even after login , docker group is not listed in `groups` command output)

3. wget docker-compose(latest link for linux_x86_64) (same for terraform)

4. append docker-compose exe path to the $PATH inn .bashrc (same for terraform)

5. in 'week_1' folder -> in '2_docker' folder, run docker-compose up -d command to install docker images

6. connect 2 docker containers using a network

7. docker network create pg-network

8. and then provide the network option for both docker containers if we're running them separately (not needed if we run using docker-compose)


## postgresql connection
1. pgcli -h localhost -U root -d ny_taxi

2. root password is root

3. PGadmin localhost:8080
    a. username => admin@admin.com 
    b. password => root

4. All about
	1. downloading csv file from nyc taxi data & AWS S3 file link
	2. loading the data into pandas python data frame
	3. connecting pandas to postgres DB and insert the data
	4. for large data, divide into chunks and load into DB


## jupyter
1. jupyter nbconvert upload-data.ipynb --to script


## gcloud setup (comes built-in with gcp machines, I guess)
1. export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/datazoomcamp.json  (this is currently in the vm machine only, in local we're not using anything but stored this json file in D:\jagadish\datazoomcamp)

2. gcloud auth application-default login (after entering the code, credentials saved in  ~/.config/gcloud/application_default_credentials.json, works on session wise)












