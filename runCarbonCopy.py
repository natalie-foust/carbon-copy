from CarbonCopy.app import DBCopyApp
import json

if __name__ == "__main__":
    with file("CarbonCopyConfig.json") as appConfigFile:
        config = json.load(appConfigFile)
    App = DBCopyApp(config)
    App.run()