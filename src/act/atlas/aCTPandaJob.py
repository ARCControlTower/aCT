import json
import os

class aCTPandaJob:
    '''
    Class containing information on a panda job being processed by aCT. Normally
    at the end of the job this information is encoded in the json file produced
    by the pilot and downloaded by aCT. aCT adjusts certain information before
    sending it to panda. If the json is missing then default values are used.
    The fields here are those expected by JobDispatcher.updateJob() in the Panda
    server.
    '''

    def __init__(self, jobinfo={}, filehandle=None, filename=''):
        '''
        Make a new aCTPandaJob. jobId and state are mandatory. jobinfo is a
        dictonary which can contain any of this class' attributes. If filehandle
        is given json info is loaded from the file object. If filename is
        given json info is read from a file.
        '''
        self.jobId = None
        self.state = None
        self.timeout = 60

        if jobinfo:
            self.setAttributes(jobinfo)
        elif filehandle:
            self.setAttributes(json.load(filehandle))
        elif filename:
            with open(filename) as f:
                self.setAttributes(json.load(f))

    def __setattr__(self, name, value):
        '''
        Override to allow setting arbitrary key value pairs
        '''
        self.__dict__[name] = value

    def setAttributes(self, jobinfo):
        '''
        Set attributes in the jobinfo dictionary
        '''
        for key, value in jobinfo.items():
            self.__dict__[key] = value

    def dictionary(self):
        '''
        Return a dictionary of all the attributes with set values
        '''
        return self.__dict__

    def writeToFile(self, filename):
        '''
        Write json of job info to filename. Overwrites an existing file.
        '''
        os.makedirs(os.path.dirname(filename), 0o755, exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(self.dictionary(), f)


if __name__ == '__main__':
    pj = aCTPandaJob(jobinfo={'jobId': 1234, 'state': 'running', 'batchID': 1234})
    pj.newfield = 'blabla'
    print(pj.__dict__)
    print(pj.dictionary())
    pj.writeToFile('/tmp/x/pickle')
    pj2 = aCTPandaJob(filename='/tmp/x/pickle')
    print(pj2.dictionary())
    with open('/tmp/x/pickle') as f:
        pj3 = aCTPandaJob(filehandle=f)
        print(pj3.dictionary())
