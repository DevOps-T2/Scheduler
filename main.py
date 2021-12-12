from typing import List
from fastapi import FastAPI, HTTPException, requests
from pydantic import BaseModel
import string
import random


class SingleComputation(BaseModel):
    solver_ids: List[float]
    mzn_id: str #The URL to that point to where the minizin model file is stored. 
    dzn_id: str #The URL that points to where the minizin data fil is stored.
    vcpus: int #The amount of Vcpu resources that this job should have
    memory: int #The amout of memory resources that this job should have
    solver_options: List[str]
    user_id: str # don't know the format that the guid is stored in.

class ComputatitonLaunch(BaseModel):
    computationLaunch: List[SingleComputation]

class checkResources:
    user_id: str
    vcpu_asked: int
    memory_asked: int

#Expected results from endpoints used for testing:

#GetQuotas:
getQuotasresult = {"memory": 10, "vCpu" : 15}
#Get current used resources for a user:

job1ForUser1 = {'id': 1, 'user_id': 1, 'computation_id': 130, 'vcpu_usage': 2, 'memory_usage': 5}
job2ForUser1 = {'id': 1, 'user_id': 1, 'computation_id': 131, 'vcpu_usage': 4, 'memory_usage': 3}
job3ForUser1 = {'id': 1, 'user_id': 1, 'computation_id': 132, 'vcpu_usage': 2, 'memory_usage': 4}

getMonitorForUserResult = [job1ForUser1, job2ForUser1, job3ForUser1]

app = FastAPI()

# Checks to see if the resources that a user asks for is available
def checkIfResourceIfavailable(request: checkResources) -> bool:
    current_vcpu_usage = 0
    current_memory_usage = 0
    available_vcpu = 0
    available_memory = 0
    limit_vcpu = 0
    limit_memory = 0
    
    #Gets the limit resources for a user, by call the GetQuotasEndPoint
    # getQuotaresult = requests.get("253.2554.546565.46545/quotas/" + request.user_id) # <-- Need to be change to the internal Cluster IP, when uploaded to Google Cloud

    # Using dummy result:
    limit_vcpu = getQuotasresult.get("vCpu")
    limit_memory = getQuotasresult.get("memory")

    # Need to call the monitor endpoint to see if the user has any jobs running
    #getMonitorForUserResult = requests.get("232652.2652.484/monitor/processes/"+ request.user_id) # <-- Need to be change to the internal Cluster IP, when uploaded to Google Cloud

    # Using dummy result:
    getMonitorForUserResult

    # Checks if the current user, has any jobs running
    if len(getMonitorForUserResult) == 0:
        current_vcpu_usage = 0
        current_memory_usage = 0

    #Calculates the current_vcpu_usage and currrent_memory_usage
    if len(getMonitorForUserResult) > 0:
        for x in getMonitorForUserResult:
            current_vcpu_usage += x.get('vcpu_usage')
            current_memory_usage += x.get('memory_usage')
    
    available_vcpu = limit_vcpu - current_vcpu_usage
    available_memory = limit_memory - current_memory_usage

    if (available_vcpu > request.vcpu_asked) and (available_memory > request.memory_asked):
        return True
    else:
        return False


# Creates a random string, to be used as a computation ID. This string is NOT unique. Default length 8
def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
   return ''.join(random.choice(chars) for _ in range(size))



#@app.post("/LanuchSingleComputation/") 
def Launch_Single_Computation(request: SingleComputation):

    if(checkIfResourceIfavailable(request.user_id, request.vcpus, request.memory)):
        
        #Post the job to the monitor Service:

        # The adress to the monitor service
        url = '2655.45445.565/monitor/process/' # <---- Needs to be change to the cluster ID 
        
        computation_id = id_generator()

        # What to be posted to the monitor service
        myjson = {'user_id': request.user_id, 'computation_id': computation_id, 'vcpu_usage': request.vcpus, 'memory_usage': request.memory}

        # The answer has the following struct accourding to code in MonitorService:
        
        '''
        class GetMonitorProcess(BaseModel):
            id: int
            user_id: str
            computation_id: str
            vcpu_usage: int
            memory_usage: int
        '''

        # Not sure, if that is correct, or the answer should just be a status code??? 

        answer = requests.post(url, json = myjson)

        if answer == 200:
            print("Job add to the monitor service")
        
        else:
            print("Job NOT add to monitor service")

        # Start minizinc solver:

        url = '5553.45454.4544/run' # <---- Needs to be change to the cluster ID

        myjson = {'model_url': request.mzn_id, 'data_url': request.dzn_id, 'solvers': request.solver_ids}

        answer = requests.post(url, json = myjson) 

    else:
        #queue the solvers


if __name__ == "__main__":

    Launch_Single_Computation()





