#SnapData
This script creates database backups of any number of databases currently stored
on AWS Relational Database Service (RDS). The script makes use of the 
automatically generated snapshots of RDS to make sure that important databases
are entirely unaffected by SnapData's running. The script never touches the
production databases, instead, it creates a read-only copy of the database and
generates a .sql file from this copy.
###Configuration
To tell the program which databases to copy you need to create a file called
**CarbonCopyConfig.json.** Inside this file there should be a json object with an
attribute *"databases"* referencing a list of objects. These objects point to the
databases you want to copy. An example **CarbonCopyConfig.json** is below:
```json
{
"databases":[
        {
        "InstanceIdentifier":"production-database",
        "Username"      : "myProductionUser",
        "Password"      : "<password-redacted>",
        "DBName"        : "ProductionDatabase",
        "DevUsername"   : "myDevelopmentUser",
        "DevPassword"   : "<password-redacted>",
        },
        
        "InstanceIdentifier":"production-database",
        "Username"      : "myProductionUser",
        "Password"      : "<password-redacted>",
        "DBName"        : "OtherProductionDatabase",
        "DevUsername"   : "myDevelopmentUser",
        "DevPassword"   : "<password-redacted>",
        }
    ]
}
```

This config file would copy the database referenced in the RDS console as
*drupal-production*. Username & Password refer to the username and password of
the database in Production. DevUsername & DevPassword on the other hand are the
***new*** passwords you are giving to the *read-only copy* of the database. This
way, we can use the *read-only duplicate database* (or the *development database*)
for testing and other purposes where we would not want to risk spreading the
production credentials.
###ALLOWED_INSTANCE_IDENTIFIERS Object
Pay special mind to changes made to this object in dbSnapshot.app. This object is
used to ensure the drastic changes this script makes only target allowed 
development databases. As long as this object is maintained properly, this
program will behave well and won't affect any production databases.
##Dependencies
###Package Dependencies (Debian Linux)
- libmysqlclient-dev
- python-pip
- awscli

### Python libary dependencies
- sqlalchemy
  - depends on mysql-python
- boto3

To install them all on Ubuntu 14.04, run these two lines of code:

```bash
sudo apt-get install python-pip libmysqlclient-dev awscli;
pip install mysql-python sqlalchemy boto3;
```


*Written and maintained by Natalie Foust, direct questions to natalie.foust.pilcher@gmail.com*