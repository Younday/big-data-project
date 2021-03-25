## Big data project

### Install Docker & Docker-compose

See link [here](https://docs.docker.com/compose/install/)

### Spin up database

Run following command:

```bash
docker-compose up -d
```

### Data
There are some misplaced tabs in the data, to fix this you have to use: 
sed -i 's/"//g' title.basics.tsv
sed -i 's/\N/0/g' title.basics.tsv

### Scripts & Queries

Python scripts can be saved in scripts folder (for data loading and keeping track of runtime). Seperate SQL or Mongo queries can be kept in queries folder.
