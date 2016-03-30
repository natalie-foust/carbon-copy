from CarbonCopy.lib import DBCopyLogger, DBCopyUsageError, LOGGER, DBCredentialsWorkWaiter
from CarbonCopy.decorators import log_method
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from botocore.exceptions import ClientError
from datetime import datetime
import subprocess, boto3, time
import sqlalchemy

FILE_PATH = "/backups/databases/"
PRODUCTION_HOSTS = []
ALLOWED_INSTANCE_IDENTIFIERS = {
# Put in your intended RDS instance ids here as the keys
#       The values should be the development equivalent ids, this ensures
#       you can't do something drastic like delete your production database, no
#       matter what kind of configuration you provide.
}


def getTimeStamp():
    return datetime.strftime(datetime.now(), "%Y-%m-%d")
def getFullTimeStamp():
    return datetime.strftime(datetime.now(), "%Y-%m-%d.%H:%M")

def isPointingAtProduction(dbConfig):
    productionIdentifiers = ALLOWED_INSTANCE_IDENTIFIERS.keys()
    return (dbConfig["InstanceIdentifier"] in productionIdentifiers)



class DBCopyApp:

    def __init__(self, config):
        self.rdsClient = boto3.client("rds")
        self.config = config
        self.logger = DBCopyLogger()

    def run(self):
        databases = self.config["databases"]
        for databaseHost in databases:
            devDatabase = self.establishNewDevDatabase(databaseHost)
            for database in databaseHost["DBName"]:              
                self.generateDumpFromDatabase(devDatabase, database)
                
    @log_method
    def establishNewDevDatabase(self, databaseConfig, *args, **kwargs):
        """
        Using the prod database credentials, create a copy of that database
        from the most recent snapshot created. This copy database allows us to
        generate a dump without ever touching the production database. We also
        change the database credentials of the development database so that
        other applications may use it without being given prod credentials.
        """

        # Assign the variables from the database configuration dictionary
        dbInstanceIdentifier = databaseConfig["InstanceIdentifier"]

        # Make sure the instance identifiers are EXPLICITLY allowed. This is a
        # safeguard to make sure that sloppy configs CANNOT result in a deleted
        # database.
        try:
            devDBInstanceIdentifier = ALLOWED_INSTANCE_IDENTIFIERS[dbInstanceIdentifier]
        except KeyError:
            message = "Database identifier not in database whitelist. Add the production:development key:value pair to the ALLOWED_INSTANCE_IDENTIFIERS constant in app.py"
            LOGGER.error(message)
            raise
        
        # If a version of our development database already exists, we grab its
        # information so that we can delete it.
        if self._doesDatabaseExist(devDBInstanceIdentifier):
            self._deleteDatabase(devDBInstanceIdentifier)

        snapshotName = self._getMostRecentSnapshot(dbInstanceIdentifier)        
        self._createDatabase(snapshotName, devDBInstanceIdentifier)        
        
        # Generate dev DB values for a return dictionary
        newDevDB = self.rdsClient.describe_db_instances(
                DBInstanceIdentifier=devDBInstanceIdentifier)["DBInstances"][0]

        devDBConfig = {
            "Host": newDevDB["Endpoint"]["Address"],
            "Username" : databaseConfig["DevUsername"],
            "Password" : databaseConfig["DevPassword"],
            "InstanceIdentifier" : devDBInstanceIdentifier
                }
        
        if not self._canConnectToDatabaseWithCredentials(devDBConfig):
            adminConfig = self._modifyAdminUser(devDBInstanceIdentifier, 
                                  databaseConfig["Password"])
            self._alterDatabaseCredentials(adminConfig, devDBConfig)
        return devDBConfig

    def generateDumpFromDatabase(self, devDBConfig, dbName):
        if isPointingAtProduction(devDBConfig):
            raise DBSnapshotUsageError("Will not generate dump directly from a production database")
        # Create db dumps from the read only version of the databse
        outputFilename = "{path}/{dbHostname}.{dbName}.{timestamp}.sql".format(
            path=FILE_PATH,
            dbHostname=devDBConfig["InstanceIdentifier"],
            dbName=dbName,
            timestamp=getTimeStamp()
        )
        tempConfig = devDBConfig.copy()
        tempConfig.update({"DBName":dbName})
        with file(outputFilename, "w") as databaseDumpFile:
            process = self._dumpSQLtoFile(tempConfig, databaseDumpFile)
            try:
                finishedWriting = False
                while not finishedWriting:
                    returnCode = self._checkOnSQLDump(process, outputFilename)
                    if returnCode is not None: finishedWriting = True
            except:
                process.terminate()
                raise
            
    @log_method
    def _dumpSQLtoFile(self, dbConfig, fileHandler):
        process = subprocess.Popen(["mysqldump",
                    "-h"+dbConfig["Host"],
                    "-u"+dbConfig["Username"],
                    "-p"+dbConfig["Password"],
                    "--quick",
                    "--single-transaction",
                    dbConfig["DBName"]
                    ],
                stdout=fileHandler,
                stdin=subprocess.PIPE)
        return process

    @log_method
    def _checkOnSQLDump(self, process, fileName):
        time.sleep(10)
        return process.poll()

    @log_method
    def _doesDatabaseExist(self, dbIdentifier):
        try:
            existing_database = self.rdsClient.describe_db_instances(
                DBInstanceIdentifier=dbIdentifier)
        except ClientError:
            existing_database = False
        return (existing_database != False)

    @log_method
    def _deleteDatabase(self, devDBInstanceIdentifier):
        availableWaiter = self.rdsClient.get_waiter("db_instance_available")
        availableWaiter.wait(DBInstanceIdentifier=devDBInstanceIdentifier)
        self.rdsClient.delete_db_instance(
            DBInstanceIdentifier=devDBInstanceIdentifier,
            SkipFinalSnapshot=True)
        
        # Block the program until the development database has been deleted        
        deletedWaiter = self.rdsClient.get_waiter("db_instance_deleted")
        deletedWaiter.wait(DBInstanceIdentifier=devDBInstanceIdentifier)

    @log_method
    def _createDatabase(self, snapshotName, devDBInstanceIdentifier):
        # Create the development database from the snapshot
        createdDatabase = self.rdsClient.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier        = devDBInstanceIdentifier,
            DBSnapshotIdentifier        = snapshotName,
            DBInstanceClass             = "db.m3.large"
        )
        
        # Block the program until the development database has been created
        availableWaiter = self.rdsClient.get_waiter("db_instance_available")
        availableWaiter.wait(DBInstanceIdentifier=devDBInstanceIdentifier)
        
        return createdDatabase
        

    def _canConnectToDatabaseWithCredentials(self, credentials):
        try:
            engine = create_engine("mysql://{username}:{password}@{host}".format(
                username    =   credentials["Username"],
                password    =   credentials["Password"],
                host        =   credentials["Host"])
            )
            engine.connect()
            return True
        except sqlalchemy.exc.OperationalError:
            return False

    @log_method
    def _alterDatabaseCredentials(self, oldDatabaseConfig, newDatabaseConfig):
        password_changed_waiter = DBCredentialsWorkWaiter(
            Username        = oldDatabaseConfig["Username"],
            Password        = oldDatabaseConfig["Password"],
            Host            = newDatabaseConfig["Host"])
        password_changed_waiter.wait()
        
        # Change the credentials of the development database to seperate
        # production credentials from our other workflows
        engine = create_engine("mysql://{username}:{password}@{host}".format(
            username    =   oldDatabaseConfig["Username"],
            password    =   oldDatabaseConfig["Password"],
            host        =   newDatabaseConfig["Host"])
        )
        Session = sessionmaker(bind=engine)
        session = Session()

        sql_statements = [
            "CREATE USER '{username}'@'%' IDENTIFIED BY '{password}';".format(
                username = newDatabaseConfig["Username"],
                password = newDatabaseConfig["Password"]),
            "GRANT SELECT ON *.* TO '{username}'@'%';".format(
                username = newDatabaseConfig["Username"]),
            "GRANT SHOW VIEW ON *.* TO '{username}'@'%';".format(
                username = newDatabaseConfig["Username"])
            ]
        for statement in sql_statements:
            session.execute(statement)
            session.commit()
            
        return newDatabaseConfig["Username"]
            
    @log_method
    def _getMostRecentSnapshot(self, productionDBIdentifier):
        dbSnapshots = self.rdsClient.describe_db_snapshots(
            DBInstanceIdentifier=productionDBIdentifier)["DBSnapshots"]
        mostRecentSnapshot = dbSnapshots[-1]
        return mostRecentSnapshot["DBSnapshotIdentifier"]
    
    @log_method
    def _modifyAdminUser(self, devDBInstanceIdentifier, adminPassword):
        dbConfig = self.rdsClient.modify_db_instance(
                DBInstanceIdentifier=devDBInstanceIdentifier,
                MasterUserPassword=adminPassword,
                DBSecurityGroups            = ["development-database"])
        return {
                "Username": dbConfig["DBInstance"]["MasterUsername"],
                "Password": adminPassword,
       }
