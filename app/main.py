import os
from typing import List
from fastapi import FastAPI, HTTPException, requests
from pydantic import BaseModel
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import string
import random

load_dotenv()

DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_HOST_READ = os.getenv("DATABASE_HOST_READ")
DATABASE_HOST_WRITE = os.getenv("DATABASE_HOST_WRITE")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")

monitorCluserIP = '123.456.789'
quotasClusterIp = '123.456.789'
minizinceClusterIp = '123.456.789'

# TO LIST for this file:
    # Quning of the jobs, is that done in the service or? (if the service handels it, should there be a database connect?) - see line 139, where I think it should be implemented 
    # Endpoint, that I (Thomas) don't know how works:
        # Minizin mzn-dispatcher
        # Solver list


# Add endpoint: That allow for Thomas to tell when a computation is finish

# All data that needs to be added to database(s)
class SingleComputation(BaseModel):
    solver_ids: List[float]
    mzn_url: str #The URL to that point to where the minizin model file is stored. 
    dzn_url: str #The URL that points to where the minizin data fil is stored.
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

# Request schema for when wanting to launch computation
class LaunchComputationRequest(BaseModel):
    solver_ids: List[int]
    mzn_id: str #The id of a minizinc instance. Pointing to a database row that includes both mzn and dzn urls
    vcpus: int #The amount of Vcpu resources that this job should have
    memory: int #The amout of memory resources that this job should have
    solver_options: List[str]
    user_id: str # don't know the format that the guid is stored in.

# Expected results from endpoints used for testing:

app = FastAPI()

# Creates a random string, to be used as a computation ID. This string is NOT unique. Default length 8
#def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
 #  return ''.join(random.choice(chars) for _ in range(size))


@app.get("/testdb") 
def test_db():
    sql_write: str = "INSERT INTO scheduler (user_id, job_memory, job_vcpu, mzn_id) values (%s, %s, %s, %s)"
    writeDB(sql_write, ("user1", 1000, 8, 0))

    sql_read: str = "SELECT * FROM scheduler"
    read_after_write = readDB(sql_read)
    print(read_after_write)

    sql_delete: str = "DELETE FROM scheduler WHERE user_id = 'user1'"
    writeDB(sql_delete)

    sql_read: str = "SELECT * FROM scheduler"
    read_after_delete = readDB(sql_read)
    print(read_after_delete)

    return "wrote, read and deleted from database"


@app.post("/LanuchSingleComputation") 
def Launch_Single_Computation(request: LaunchComputationRequest):

    if(checkIfResourceIfavailable(request.user_id, request.vcpus, request.memory)):
        
        # Start minizinc solver:

        # The adress to the monitor service
        url = minizinceClusterIp + '/run' 

        # The request 
        myjson = {'model_url': request.mzn_id, 'data_url': request.dzn_id, 'solvers': request.solver_ids}

        computation_id = requests.post(url, json = myjson) 

        #Post the job to the monitor Service:

        # The adress to the monitor service
        url = monitorCluserIP + '/monitor/process/'  
  
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
    
        return computation_id

    #Checks to see if the requested job is 1 jobs with too many solvers to run in prallel. Meaning solvers > limit resources for just this job.
    # Then the jobs should be can canceled

    getQuotasresult = get_user_quota(request.user_id)

    limit_vcpu = getQuotasresult.get("vCpu")

    if len(request.solver_ids) > limit_vcpu:

        return "The job could not be created, doe to that the total amount of solver exceed the number of vCPUs available"
        
    else:
        # TO DO: queue the solvers 
        
        return


# Calls the monitorservie and deleths the job from it.
@app.post("/sceduler/FinishComputation")
def finish_computation(computationID : str):

    answer = requests.delete(monitorCluserIP + '/monitor/process/' + computationID)

    print("The job has been deleth")

if __name__ == "__main__":

    Launch_Single_Computation()

# Checks to see if the resources that a user asks for is available
def checkIfResourceIfavailable(request: checkResources) -> bool:
    # initialize values to allow increments later
    current_vcpu_usage = 0
    current_memory_usage = 0
    
    #Gets the limit resources for a user, by call the GetQuotasEndPoint
    getQuotaResult = get_user_quota(request.user_id)
    limit_vcpu = getQuotaResult.get("vCpu")
    limit_memory = getQuotaResult.get("memory")

    # A list of monitored processes
    getMonitorForUserResult = get_user_monitor_processes(request.user_id)

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

def schedule_job(job: SingleComputation):
    """Adds job to queue

    Args:
        job (SingleComputation): The job to be scheduled
    """


    return

def get_user_quota(user_id: str):
    # GetQuota:
    #getQuotaResult = requests.get("253.2554.546565.46545/quotas/" + request.user_id) # <-- Need to be change to the internal Cluster IP, when uploaded to Google Cloud
    getQuotaResult = {"memory": 10, "vCpu" : 15}

    return getQuotaResult

def get_mzn_instance(mzn_id: int):
    

    return

def get_user_monitor_processes(user_id: str):
    # Get current used resources for a user:
    # Need to call the monitor endpoint to see if the user has any jobs running
    #getMonitorForUserResult = requests.get("232652.2652.484/monitor/processes/"+ request.user_id) # <-- Need to be change to the internal Cluster IP, when uploaded to Google Cloud
    getMonitorForUserResult = [
        {'id': 1, 'user_id': 1, 'computation_id': 130, 'vcpu_usage': 2, 'memory_usage': 5}, 
        {'id': 1, 'user_id': 1, 'computation_id': 131, 'vcpu_usage': 4, 'memory_usage': 3}, 
        {'id': 1, 'user_id': 1, 'computation_id': 132, 'vcpu_usage': 2, 'memory_usage': 4}
        ]

    return getMonitorForUserResult

def writeDB(sql_prepared_statement: str, sql_placeholder_values: tuple = ()):
    """Takes a prepared statement with values and writes to database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().
    """
    connection = mysql.connector.connect(database=DATABASE_NAME,
                                         host=DATABASE_HOST_WRITE,
                                         user=DATABASE_USER,
                                         password=DATABASE_PASSWORD
                                         )

    try:
        if (connection.is_connected()):
            cursor = connection.cursor(prepared=True)
            cursor.execute(sql_prepared_statement, sql_placeholder_values)
            connection.commit()
    except Error as e:
        raise HTTPException(
            status_code=500, detail="Error while contacting database. " + str(e))
    finally:
        cursor.close()
        connection.close()


def readDB(sql_prepared_statement: str, sql_placeholder_values: tuple = ()):
    """Takes a prepared statement with values and makes a query to the database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().

    Returns:
        List(tuple): The fetched result
    """
    connection = mysql.connector.connect(database=DATABASE_NAME,
                                         host=DATABASE_HOST_READ,
                                         user=DATABASE_USER,
                                         password=DATABASE_PASSWORD
                                         )
    try:
        if (connection.is_connected()):
            cursor = connection.cursor(prepared=True)
            cursor.execute(sql_prepared_statement, sql_placeholder_values)
            result = cursor.fetchall()
            return result
    except Error as e:
        raise HTTPException(
            status_code=500, detail="Error while contacting database. " + str(e))
    finally:
        cursor.close()
        connection.close()



