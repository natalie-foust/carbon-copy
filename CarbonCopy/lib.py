from sqlalchemy import create_engine
import logging, math, os, sqlalchemy, time
from logging import DEBUG, INFO, ERROR, WARN

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

class DBCopyUsageError(StandardError):
    pass


class BaseLogger:
    def log(self, messageList):
        if not messageList: return
        for level, message in messageList:
            LOGGER.log(level, message)
            
    def runMethodIfExists(self, methodName, extraData, 
                           *args, **kwargs):
        nullFunction = lambda *args, **kwargs: None
        method = getattr(self, methodName, nullFunction)
        if extraData != None:
            message = method(extraData, *args, **kwargs)
        else:
            message = method(*args, **kwargs)
        self.log(message)


class DBCopyLogger(BaseLogger):
    """
    A class to handle logging for DBSnapshotApp. Contains shorthand ways to
    reference logging statements to keep DBSnapshotApp's code light and 
    readable
    """
    def establishNewDevDatabase(self, databaseConfig, *args, **kwargs):
        message = []
        message.append((INFO,
            "Creating an independent development database based off of '{productionDB}'".format(
                productionDB=databaseConfig["InstanceIdentifier"])
            )
        )
        return message
        
    def establishNewDevDatabaseFinished(self, returnValues, databaseConfig, 
                            *args, **kwargs):
        message = []
        message.append((INFO,
            "Successfully created new development database '{devDBName}'.".format(
                devDBName=returnValues["InstanceIdentifier"]))
        )
        return message
    
    def _deleteDatabase(self, developmentDatabase, snapshotName=False, *args, **kwargs):
        message = []
        message.append((INFO, 
            "Deleting development database '{instanceID}'...".format(
                instanceID=developmentDatabase)
        ))
        if not snapshotName:
            message.append((WARN, "Not saving database to any snapshot."))
        else:
            message.append((INFO, "Saving final copy of database to snapshot {snapshotID}".format(snapshotID=snapshotName)))
        message.append((INFO, "This may take a few minutes..."))
        return message
        
    def _deleteDatabaseFinished(self, _, *args, **kwargs):
        message = []
        message.append((INFO, "Database deleted successfully"))
        return message

        
    def _deleteDatabaseFailed(self, error, *args, **kwargs):
        message = []
        message.append((ERROR, "Could not delete database."))
        message.append((ERROR, error.message))
        return message
        
    def _doesDatabaseExist(self, *args, **kwargs):
        message = []
        message.append((INFO, "Checking if development database already exists..."))
        return message
        
    def _doesDatabaseExistFinished(self, returnValues, *args, **kwargs):
        message = []
        if returnValues:
            message.append((INFO, "Development database currently exists, preparing database for deletion.."))
        else:
            message.append((INFO, "Development database does not exist yet."))
        return message
    
    def _createDatabase(self, snapshotName, devDBInstanceIdentifier, 
                        *args, **kwargs):
        message = []
        message.append((INFO, "Creating fresh development database '{devInstanceID}' \
from most recent snapshot of production database".format(
            devInstanceID       =       devDBInstanceIdentifier)
        ))
        message.append((INFO, "This may take a few minutes..."))
        
        return message

    def _createDatabaseFinished(self, returnArgs, snapshotName,
                                devDBInstanceIdentifier, *args, **kwargs):
        message = []
        message.append((INFO, "Fresh development database '{devInstanceID}' created".format(
                devInstanceID   =       devDBInstanceIdentifier)))
        return message

    def _createDatabaseFailed(self, error, snapshotName, 
                              devDBInstanceIdentifier="N/A", *args, **kwargs):
        message = []
        message.append((ERROR, "Failed to create development database '{devInstanceID}'".format(
            devInstanceID       =       devDBInstanceIdentifier)))
        message.append((ERROR, error.message))
        return message

    def _alterDatabaseCredentials(self, *args, **kwargs):
        message = []
        message.append((INFO, "Adding new read-only database credentials for security."))
        message.append((INFO, "This may take some time..."))
        return message
        
    def _alterDatabaseCredentialsFinished(self, returnValues, *args, **kwargs):
        message = []
        message.append((INFO, "Finished adding new read-only user '{username}'.".format(
            username = returnValues)
        ))
        return message

    def _alterDatabaseCredentialsFailed(self, error, *args, **kwargs):
        message = []
        message.append((ERROR, "Failed to alter database credentials."))
        message.append((ERROR, error.message))
        return message

    def _checkOnSQLDump(self, process, filename, *args, **kwargs):
        message = []

        totalBytes = os.path.getsize(filename)
        totalKB = round(totalBytes/1024., 2)
        totalMB = round(totalBytes/1048576., 2)
        totalGB = round(totalBytes/1073741824., 2)
        if totalGB >= 1.0:
            prettyBytes = str(totalGB)+"GB"
        elif totalMB >= 1.0:
            prettyBytes = str(totalMB)+"MB"
        elif totalKB >= 1.0:
            prettyBytes = str(totalKB)+"KB"
        else:
            prettyBytes = str(totalBytes)+" Bytes"
            
        message.append((DEBUG, "Currently written {humanReadableBytes}".format(
            humanReadableBytes = prettyBytes)))
        return message
        
    def _dumpSQLtoFile(self, config, fileHandler, *args, **kwargs):
        message = []
        filename = fileHandler.name
        message.append((INFO, "Dumping database to file {filename}".format(
            filename = filename)))
        return message

    def _checkOnSQLDumpFailed(self, error, *args, **kwargs):
        message = []
        message.append((ERROR, error.message))
        return message
    
    def _getMostRecentSnapshotFinished(self, snapshotName,
                                       productionDBIdentifier, *args, **kwargs):
        message =[]
        message.append((INFO, "Selecting '{snapshotName}' as most recent snapshot of '{productionDB}'".format(
            snapshotName = snapshotName,
            productionDB = productionDBIdentifier)
        ))
        return message

    def _modifyAdminUserFinished(self, returnValues,
                                 devDBInstanceIdentifier, _, *args, **kwargs):
        message = []
        message.append((INFO, "Changing to the password of the admin user '{adminUsername}' of new database '{devDBInstanceIdentifier}' to the production password.".format(
            adminUsername = returnValues["Username"],
            devDBInstanceIdentifier = devDBInstanceIdentifier
        ))
        )
        return message

class DBCredentialsWorkWaiter:
    def __init__(self, Username, Password, Host):
        self.sqlURL = "mysql://{username}:{password}@{host}".format(
            username    =   Username,
            password    =   Password,
            host        =   Host
        )
        
    def wait(self):
        attempts = 0
        waiting = True
        while waiting:
            if attempts>=10: break
            try:
                self._connect()
                waiting=False
            except sqlalchemy.exc.OperationalError:
                time.sleep(45)
            attempts += 1
        
    def _connect(self):
        engine = create_engine(self.sqlURL)
        engine.connect()