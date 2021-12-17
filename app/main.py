import os
from typing import Dict, List
from fastapi import FastAPI, HTTPException, requests
from pydantic import BaseModel
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import string
import random

load_dotenv()

# database credentials
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_HOST_READ = os.getenv("DATABASE_HOST_READ")
DATABASE_HOST_WRITE = os.getenv("DATABASE_HOST_WRITE")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")

# service ips
MONITOR_SERVICE_IP = os.getenv("MONITOR_SERVICE_IP")
QUOTA_SERVICE_IP = os.getenv("QUOTA_SERVICE_IP")
MZN_SERVICE_IP = os.getenv("MZN_SERVICE_IP")


# TO DO LIST for this file:
    # Quning of the computations, is that done in the service or? (if the service handels it, should there be a database connect?) - see line 139, where I think it should be implemented 
    # Endpoint, that I (Thomas) don't know how works:
        # Minizin mzn-dispatcher
        # Solver list


# Add endpoint: That allow for Thomas to tell when a computation is finish

# All data that needs to be added to the database
class Computation(BaseModel):
    solver_ids: List[int]
    mzn_url: str #The URL to that point to where the minizin model file is stored. 
    dzn_url: str #The URL that points to where the minizin data fil is stored.
    vcpus: int #The amount of Vcpu resources that this computation should have
    memory: int #The amout of memory resources that this computation should have
    solver_options: List[str]
    user_id: str # don't know the format that the guid is stored in.

class CheckResources:
    user_id: str
    vcpu_requested: int
    memory_requested: int

# Request schema for when wanting to launch computation
class LaunchComputationRequest(BaseModel):
    solver_ids: List[int]
    mzn_id: str #The id of a minizinc instance. Pointing to a database row that includes both mzn and dzn urls
    vcpus: int #The amount of Vcpu resources that this computation should have
    memory: int #The amout of memory resources that this computation should have
    solver_options: List[str]
    user_id: str # don't know the format that the guid is stored in.

class FinishComputationRequest(BaseModel):
    user_id: str
    computation_id: str

# Expected results from endpoints used for testing:

# Creates a random string, to be used as a computation ID. This string is NOT unique. Default length 8
#def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
 #  return ''.join(random.choice(chars) for _ in range(size))

app = FastAPI()

@app.get("/testdb") 
def test_db():
    computation = Computation(solver_ids = [1,2,3], 
                    solver_options = [], 
                    mzn_url = "url", 
                    dzn_url = "url", 
                    user_id = "user1", 
                    memory=2000, 
                    vcpus = 2)

    # schedule_computation(computation)
    # print(get_all_user_scheduled_computations(computation.user_id))
    # delete_scheduled_computation("24")
    launch_scheduled_computation("user1")


    return "wrote, read and deleted from database"


@app.post("/scheduler/computation") 
def create_computation(request: LaunchComputationRequest):
    # check if the computation request is ever runnable with the user's cpu quota
    user_quota = get_user_quota(request.user_id)
    limit_vcpu = user_quota.get("vcpu")
    if len(request.solver_ids) > limit_vcpu:
        raise HTTPException(status_code=403, detail="""The requested computation can never be launched, 
                                                    because the requested amount of parallel solvers (%s) 
                                                    exceeds the user's quota for vCPUs (%s)""" 
                                                    % (len(request.solver_ids), limit_vcpu))

    # get mzn urls from mzn id
    mzn_data = get_mzn_instance(request.mzn_id)

    # create a computation object with both mzn/dzn urls and the request data
    computation = Computation(solver_ids = request.solver_ids, 
                            solver_options = request.solver_options, 
                            mzn_url = mzn_data.mzn_url, 
                            dzn_url = mzn_data.dzn_url, 
                            user_id = request.user_id, 
                            memory = request.memory, 
                            vcpus = request.vcpus)

    scheduled_computations: List = get_all_user_scheduled_computations(computation.user_id)

    if (len(scheduled_computations) == 0 and user_resources_are_available(computation.user_id, computation.vcpus, computation.memory)):
        launch_computation(computation)
    else:
        schedule_computation(computation)

@app.delete("/scheduler/computations/{user_id}") 
def delete_user_computations(user_id: str):
    scheduled_computations = get_all_user_scheduled_computations(user_id)
    
    for scheduled_computation in scheduled_computations:
        scheduled_computation_id = scheduled_computation.get("scheduler_id")
        delete_scheduled_computation(scheduled_computation_id)

    return "Deleted all scheduled computations associated with user"

@app.post("/scheduler/finish_computation")
def finish_computation(request: FinishComputationRequest):
    """Takes a message from the solver execution service, singalling an execution has terminated
    Deletes the process from the monitor service and launches the next scheduled computation

    Args:
        request (FinishComputationRequest): a computation id and a user id
    """
    delete_process_response = requests.delete(MONITOR_SERVICE_IP + '/monitor/process/' + request.computation_id)
    launch_scheduled_computation(request.user_id)


# Checks to see if the resources that a user asks for is available
def user_resources_are_available(request: CheckResources) -> bool:
    # initialize values to allow increments later
    current_vcpu_usage = 0
    current_memory_usage = 0
    
    #Gets the limit resources for a user, by call the GetQuotasEndPoint
    getQuotaResult = get_user_quota(request.user_id)
    limit_vcpu = getQuotaResult.get("vcpu")
    limit_memory = getQuotaResult.get("memory")

    # A list of monitored processes
    getMonitorForUserResult = get_user_monitor_processes(request.user_id)

    # Checks if the current user, has any computations running
    if len(getMonitorForUserResult) == 0:
        return

    #Calculates the current_vcpu_usage and currrent_memory_usage
    if len(getMonitorForUserResult) > 0:
        for x in getMonitorForUserResult:
            current_vcpu_usage += x.get('vcpu_usage')
            current_memory_usage += x.get('memory_usage')
    
    available_vcpu = limit_vcpu - current_vcpu_usage
    available_memory = limit_memory - current_memory_usage

    if (available_vcpu > request.vcpu_requested) and (available_memory > request.memory_requested):
        return True
    else:
        return False

def launch_scheduled_computation(user_id):
    """Find oldest scheduled computation and launch it after deleting it from the "queue".

    Args:
        user_id (str): the id of the user
    """
    scheduled_computations = get_all_user_scheduled_computations(user_id)

    if (len(scheduled_computations) == 0):
        return

    oldest_scheduled_computation = min(scheduled_computations, key=lambda dict: dict["scheduler_id"])

    delete_scheduled_computation(oldest_scheduled_computation.get("scheduler_id"))
    launch_computation(oldest_scheduled_computation.get("computation"))

def launch_computation(computation: Computation):
    """Contact solver execution service and launch an actual execution / computation

    Args:
        computation (Computation): All the info the solver execution service needs

    Returns:
        [type]: [description]
    """
    # Start minizinc solver: 
    solver_execution_request = {'model_url': computation.mzn_url, 'data_url': computation.dzn_url, 'solvers': computation.solver_ids}

    solver_execution_response = requests.post(MZN_SERVICE_IP + '/run', json = solver_execution_request) 
    solver_execution_response_body = solver_execution_response.json()
    computation_id = solver_execution_response_body.get("computation_id")

    # Post the computation to the monitor Service: 
    # The request body
    monitor_request = {'user_id': computation.user_id, 'computation_id': computation_id, 'vcpu_usage': computation.vcpus, 'memory_usage': computation.memory}

    monitor_response = requests.post(MONITOR_SERVICE_IP + '/monitor/process/', json = monitor_request)

    if monitor_response.status_code == 200:
        print("computation add to the monitor service")
    else:
        print("computation NOT add to monitor service")

    return computation_id
    

def schedule_computation(computation: Computation):
    """Adds computation to queue

    Args:
        computation (Computation): The computation to be scheduled
    """
    scheduler_prepared_sql: str = "INSERT INTO scheduler (user_id, memory_usage, vcpu_usage, mzn_url, dzn_url) values (%s, %s, %s, %s, %s)"
    schduler_values = (computation.user_id, computation.memory, computation.vcpus, computation.mzn_url, computation.dzn_url)

    # write data to scheduler table and return the auto incremented id 
    inserted_row_scheduler_id = writeDB(scheduler_prepared_sql, schduler_values)

    # write all the solver_ids to the scheduler_solver table
    scheduler_solver_prepared_sql: str = "INSERT INTO scheduler_solver (scheduler_id, solver_id) values (%s, %s)"
    for solver_id in computation.solver_ids:
        writeDB(scheduler_solver_prepared_sql, (inserted_row_scheduler_id, solver_id))

def load_scheduled_computation(scheduler_id: int) -> Computation:
    # get all solver ids and save in a list
    scheduler_solver_prepared_sql: str = "SELECT solver_id FROM scheduler_solver WHERE scheduler_id = %s" 
    solver_id_tuples = readDB(scheduler_solver_prepared_sql, (scheduler_id,))
    solver_ids = [id_tuple[0] for id_tuple in solver_id_tuples]

    # get the rest of the data and save it in an object along with solver ids
    scheduler_prepared_sql: str = "SELECT user_id, memory_usage, vcpu_usage, mzn_url, dzn_url FROM scheduler WHERE id = %s"
    result = readDB(scheduler_prepared_sql, (scheduler_id,))[0]

    scheduled_computation = Computation(user_id = result[0], 
                                    memory = result[1], 
                                    vcpus = result[2],
                                    mzn_url = result[3],
                                    dzn_url = result[4],
                                    solver_ids=solver_ids,
                                    solver_options = [])

    return scheduled_computation

def delete_scheduled_computation(scheduler_id: int):
    scheduler_prepared_sql: str = "DELETE FROM scheduler WHERE id = %s"
    writeDB(scheduler_prepared_sql, (scheduler_id,))

    scheduler_solver_prepared_sql: str = "DELETE FROM scheduler_solver WHERE scheduler_id = %s"
    writeDB(scheduler_solver_prepared_sql, (scheduler_id,))

def get_all_user_scheduled_computations(user_id: str) -> List[Computation]:
    scheduler_prepared_sql: str = "SELECT id FROM scheduler WHERE user_id = %s"
    scheduler_values = (user_id,)

    scheduler_id_tuples: List[tuple] = readDB(scheduler_prepared_sql, scheduler_values)
    scheduler_ids = [id_tuple[0] for id_tuple in scheduler_id_tuples] # map list of tuples to list of ints

    scheduled_computations = []
    for scheduler_id in scheduler_ids:
        scheduled_computations.append({"scheduler_id": scheduler_id, "computation": load_scheduled_computation(scheduler_id)})

    return scheduled_computations

def get_user_quota(user_id: str) -> Dict[int, int]:
    # GetQuota:
    getQuotaResult = requests.get(QUOTA_SERVICE_IP + "/quotas/" + user_id)
    getQuotaResult = {"memory": 10, "vcpu" : 15}

    return getQuotaResult

def get_mzn_instance(mzn_id: int):
    # get mzn and dzn urls from mzn_instance table in mzn_data service

    mzn_instance_response = {"mzn_url": "www.mznurl.com", "dzn_url": "www.dznurl.com"}

    return mzn_instance_response

def get_user_monitor_processes(user_id: str):
    # Get current used resources for a user:
    # Need to call the monitor endpoint to see if the user has any computations running
    getMonitorForUserResult = requests.get(MONITOR_SERVICE_IP + "/monitor/processes/"+ user_id)
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

    lastrowid = 0
    try:
        if (connection.is_connected()):
            cursor = connection.cursor(prepared=True)
            cursor.execute(sql_prepared_statement, sql_placeholder_values)
            connection.commit()
            lastrowid = cursor.lastrowid
    except Error as e:
        raise HTTPException(
            status_code=500, detail="Error while contacting database. " + str(e))
    finally:
        cursor.close()
        connection.close()

    return lastrowid

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



