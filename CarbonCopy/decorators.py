import sys

def log_method(wrappedFunction, *args, **kwargs):
    def loggedFunction(self, *args, **kwargs):
        functionName = wrappedFunction.func_name
        self.logger.runMethodIfExists(functionName, None, *args, **kwargs)
        try:
            returnValues = wrappedFunction(self, *args, **kwargs)
            
        except Exception as e:
            failedFunctionName = functionName + "Failed"
            self.logger.runMethodIfExists(failedFunctionName, e,
                                      *args, **kwargs)
            raise
        else:
            finishedFunctionName = functionName + "Finished"
            self.logger.runMethodIfExists(finishedFunctionName, 
                                          returnValues,
                                          *args, **kwargs)
        return returnValues
    return loggedFunction
