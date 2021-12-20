import os
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel, validator
import requests
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

# database credentials
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_HOST_READ = os.getenv("DATABASE_HOST_READ")
DATABASE_HOST_WRITE = os.getenv("DATABASE_HOST_WRITE")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = '' # os.getenv("DATABASE_PASSWORD")

# service ips
MONITOR_SERVICE_IP = os.getenv("MONITOR_SERVICE_IP")
QUOTA_SERVICE_IP = os.getenv("QUOTA_SERVICE_IP")
MZN_SERVICE_IP = os.getenv("MZN_SERVICE_IP")
MZN_DATA_SERVICE_IP = os.getenv("MZN_DATA_SERVICE_IP")
SOLVERS_SERVICE_IP = os.getenv("SOLVERS_SERVICE_IP")

headers = { 'UserId': 'system', 'Role': 'admin'}

# All data that needs to be added to the database
class ScheduleComputationRequest(BaseModel):
    solver_ids: List[int]
    mzn_file_id: str #The URL to that point to where the minizin model file is stored. 
    dzn_file_id: Optional[str] #The URL that points to where the minizin data fil is stored.
    vcpus: int #The amount of Vcpu resources that this computation should have
    memory: int #The amout of memory resources that this computation should have
    user_id: str # don't know the format that the guid is stored in.

    @validator('vcpus')
    def check_vcpu_more_than_one(cls, v):
        if (v < 1):
            raise ValueError ("vcpus can't be less than 1")
        return v

    @validator('memory')
    def check_memory_more_than_one(cls, v):
        if (v < 1):
            raise ValueError ("memory can't be less than 1")
        return v

# Same as ScheduleComputationRequest but with the autoincremented id
class ScheduledComputationResponse(BaseModel):
    id: int
    solver_ids: List[int]
    mzn_file_id: str #The file id used to create dzn file url 
    dzn_file_id: str #The file id used to create mzn file url 
    vcpus: int #The amount of Vcpu resources that this computation should have
    memory: int #The amout of memory resources that this computation should have
    user_id: str # don't know the format that the guid is stored in.

# model for launching a computation. mzn attributes are now urls, not ids
class LaunchComputationResponse(BaseModel):
    solver_ids: List[int]
    mzn_file_url: str #The URL to that point to where the minizin model file is stored. 
    dzn_file_url: str #The URL that points to where the minizin data fil is stored.
    vcpus: int #The amount of Vcpu resources that this computation should have
    memory: int #The amout of memory resources that this computation should have
    user_id: str # don't know the format that the guid is stored in.

# Data needed from solverexecution when it notifies about finishing a computaiton
class FinishComputationMessage(BaseModel):
    user_id: str
    computation_id: str

class Solver(BaseModel):
    image: str
    cpu_request: int
    mem_request: int
    timeout_seconds: int

app = FastAPI()
router = APIRouter()

@router.post("/api/scheduler/computation") 
@router.post("/api/scheduler/computation/", include_in_schema=False)
def create_computation(request: ScheduleComputationRequest):
    # check if the computation request is ever runnable with the user's quota
    user_quota = get_user_quota(request.user_id)
    print("user quota:", user_quota)
    limit_vcpu = user_quota.get("vCpu")
    limit_memory = user_quota.get("memory")

    print(limit_memory)
    print(limit_vcpu)

    print(request)
    print(request.vcpus)

    if (len(request.solver_ids) > limit_vcpu):
        raise HTTPException(status_code=403, detail="""The requested computation can never be launched, 
                                                    because the requested amount of parallel solvers (%s) 
                                                    exceeds the user's vCPU quota (%s)""" 
                                                    % (len(request.solver_ids), limit_vcpu))
    if (request.vcpus > limit_vcpu):
        raise HTTPException(status_code=403, detail="""The requested computation can never be launched, 
                                                    because the requested amount of vCPUs (%s) 
                                                    exceeds the user's vCPU quota (%s)""" 
                                                    % (request.vcpus, limit_vcpu))
    if (request.memory > limit_memory):
        raise HTTPException(status_code=403, detail="""The requested computation can never be launched, 
                                                    because the requested amount of memory (%s) 
                                                    exceeds the user's memory quota (%s)""" 
                                                    % (request.memory, limit_memory))

    # create a computation object with both mzn/dzn urls and the request data
    computation = ScheduleComputationRequest(solver_ids = request.solver_ids, 
                            mzn_file_id = request.mzn_file_id, 
                            dzn_file_id = request.dzn_file_id, 
                            user_id = request.user_id, 
                            memory = request.memory, 
                            vcpus = request.vcpus)

    scheduled_computations: List = get_all_user_scheduled_computations(computation.user_id)

    if (len(scheduled_computations) == 0 and user_resources_are_available(computation.user_id, computation.vcpus, computation.memory)):
        computation_id = launch_computation(computation)
        return "Computation has been launched immediately with computation id: %s" % computation_id
    else:
        schedule_computation(computation)
        return "Computation has been scheduled for launch"

@router.delete("/api/scheduler/computation/{scheduled_computation_id}")
@router.delete("/api/scheduler/computation/{scheduled_computation_id}/", include_in_schema=False) 
def delete_computation(scheduled_computation_id):
    delete_scheduled_computation(scheduled_computation_id)

    return "Scheduled computation '%s' has been unscheduled" % scheduled_computation_id

@router.get("/api/scheduler/computations/{user_id}", response_model=List[ScheduledComputationResponse])
@router.get("/api/scheduler/computations/{user_id}/", response_model=List[ScheduledComputationResponse], include_in_schema=False) 
def list_user_computations(user_id: str):
    scheduled_computations = get_all_user_scheduled_computations(user_id)
    return scheduled_computations

@router.delete("/api/scheduler/computations/{user_id}")
@router.delete("/api/scheduler/computations/{user_id}/", include_in_schema=False) 
def delete_user_computations(user_id: str):
    scheduled_computations = get_all_user_scheduled_computations(user_id)
    len(scheduled_computations)

    for scheduled_computation in scheduled_computations:
        scheduled_computation_id = scheduled_computation.id
        delete_scheduled_computation(scheduled_computation_id)

    return "Deleted all (%s) scheduled computations associated with user %s" % (len(scheduled_computations), user_id)

@router.post("/api/scheduler/finish_computation")
@router.post("/api/scheduler/finish_computation/", include_in_schema=False)
def finish_computation(request: FinishComputationMessage):
    """Takes a message from the solver execution service, singalling an execution has terminated
    Deletes the process from the monitor service and launches the next scheduled computation

    Args:
        request (FinishComputationMessage): a computation id and a user id
    """
    delete_process_response = "" #requests.delete(MONITOR_SERVICE_IP + '/monitor/process/' + request.computation_id)
    return launch_scheduled_computation(request.user_id)

app.include_router(router)

# Checks to see if the resources that a user asks for is available
def user_resources_are_available(user_id, vcpu_requested, memory_requested) -> bool:
    # initialize values to allow increments later
    current_vcpu_usage = 0
    current_memory_usage = 0
    
    #Gets the limit resources for a user, by call the GetQuotasEndPoint
    getQuotaResult = get_user_quota(user_id)
    limit_vcpu = getQuotaResult.get("vCpu")
    limit_memory = getQuotaResult.get("memory")

    print("limit_vcpu: ", limit_vcpu)
    print("limit_memory: ", limit_memory)

    # A list of monitored processes
    getMonitorForUserResult = get_user_monitor_processes(user_id)

    #Calculates the current_vcpu_usage and currrent_memory_usage
    if len(getMonitorForUserResult) > 0:
        for x in getMonitorForUserResult:
            current_vcpu_usage += x.get('vcpu_usage')
            current_memory_usage += x.get('memory_usage')
    
    available_vcpu = limit_vcpu - current_vcpu_usage
    available_memory = limit_memory - current_memory_usage

    if (available_vcpu >= vcpu_requested) and (available_memory >= memory_requested):
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
        return "User %s has not scheduled computations" % user_id

    oldest_scheduled_computation = min(scheduled_computations, key=lambda x: x.id)

    delete_scheduled_computation(oldest_scheduled_computation.id)
    return launch_computation(oldest_scheduled_computation)

def launch_computation(computation: ScheduleComputationRequest):
    """Contact solver execution service and launch an actual execution / computation

    Args:
        computation (Computation): All the info the solver execution service needs

    Returns:
        [type]: [description]
    """

    # get urls by file_id
    mzn_url = get_mzn_url(computation.user_id, computation.mzn_file_id)

    if (computation.dzn_file_id != None):
        dzn_url = get_mzn_url(computation.user_id, computation.dzn_file_id)

    # construct solver objects (calculate resource fractions and get solver image from solver image id)
    solvers = []
    for solver_id in computation.solver_ids:
        solver_image = get_solver_image(solver_id)
        vcpu_fraction = int(computation.vcpus / len(computation.solver_ids))
        memory_fraction = int(computation.memory / len(computation.solver_ids))
        solver = Solver(image = solver_image, cpu_request = vcpu_fraction, mem_request = memory_fraction, timeout_seconds = 30)
        solver_json = solver.json()
        solvers.append(solver_json)

    # Start minizinc solver: 
    solver_execution_request = {'user_id': computation.user_id, 'model_url': mzn_url, 'data_url': dzn_url, 'solvers': solvers}
    solver_execution_response = None
    try:
        solver_execution_response = requests.post("http://" + MZN_SERVICE_IP + ':8080/run', json = solver_execution_request, headers=headers)     
    except Error as e:
        return e
    
    solver_execution_response_body = solver_execution_response.json()
    computation_id = solver_execution_response_body.get("computation_id")

    # Post the computation to the monitor Service: 
    monitor_request = {'user_id': computation.user_id, 'computation_id': computation_id, 'vcpu_usage': computation.vcpus, 'memory_usage': computation.memory}

    monitor_response = None
    try:
        monitor_response = requests.post("http://" + MONITOR_SERVICE_IP + '/monitor/process/', json = monitor_request, headers=headers)
    except Error as e:
        return e

    return {"computation_id": computation_id}
    

def schedule_computation(computation: ScheduleComputationRequest):
    """Adds computation to queue

    Args:
        computation (Computation): The computation to be scheduled
    """
    scheduledcomputation_prepared_sql: str = "INSERT INTO scheduledcomputation (user_id, memory_usage, vcpu_usage, mzn_file_id, dzn_file_id) values (%s, %s, %s, %s, %s)"
    scheduledcomputation_values = (computation.user_id, computation.memory, computation.vcpus, computation.mzn_file_id, computation.dzn_file_id)

    # write data to scheduledcomputation table and return the auto incremented id 
    inserted_row_scheduledcomputation_id = writeDB(scheduledcomputation_prepared_sql, scheduledcomputation_values)

    # write all the solver_ids to the scheduledcomputation_solver table
    scheduledcomputation_solver_prepared_sql: str = "INSERT INTO scheduledcomputation_solver (scheduledcomputation_id, solver_id) values (%s, %s)"
    for solver_id in computation.solver_ids:
        writeDB(scheduledcomputation_solver_prepared_sql, (inserted_row_scheduledcomputation_id, solver_id))

def load_scheduled_computation(scheduledcomputation_id: int) -> ScheduledComputationResponse:
    # get all solver ids and save in a list
    scheduledcomputation_solver_prepared_sql: str = "SELECT solver_id FROM scheduledcomputation_solver WHERE scheduledcomputation_id = %s" 
    solver_id_tuples = readDB(scheduledcomputation_solver_prepared_sql, (scheduledcomputation_id,))

    if (len(solver_id_tuples) == 0):
        return None

    solver_ids = [id_tuple[0] for id_tuple in solver_id_tuples]

    # get the rest of the data and save it in an object along with solver ids
    scheduledcomputation_prepared_sql: str = "SELECT id, user_id, memory_usage, vcpu_usage, mzn_file_id, dzn_file_id FROM scheduledcomputation WHERE id = %s"
    query_result = readDB(scheduledcomputation_prepared_sql, (scheduledcomputation_id,))
    if (len(query_result) == 0):
        return None

    scheduled_computation = query_result[0]
    scheduled_computation = ScheduledComputationResponse(id = scheduled_computation[0], 
                                    user_id = scheduled_computation[1], 
                                    memory = scheduled_computation[2], 
                                    vcpus = scheduled_computation[3],
                                    mzn_file_id = scheduled_computation[4],
                                    dzn_file_id = scheduled_computation[5],
                                    solver_ids=solver_ids)

    return scheduled_computation

def delete_scheduled_computation(scheduled_computation_id: int):
    scheduled_computation = load_scheduled_computation(scheduled_computation_id)
    if (scheduled_computation == None):
        HTTPException(
            status_code=404, detail="A process with scheduled_computation_id = '%s' does not exist." % scheduled_computation_id)

    scheduledcomputation_prepared_sql: str = "DELETE FROM scheduledcomputation WHERE id = %s"
    writeDB(scheduledcomputation_prepared_sql, (scheduled_computation_id,))

    scheduledcomputation_solver_prepared_sql: str = "DELETE FROM scheduledcomputation_solver WHERE scheduledcomputation_id = %s"
    writeDB(scheduledcomputation_solver_prepared_sql, (scheduled_computation_id,))

def get_all_user_scheduled_computations(user_id: str) -> List[ScheduledComputationResponse]:
    scheduledcomputation_prepared_sql: str = "SELECT id FROM scheduledcomputation WHERE user_id = %s"
    scheduledcomputation_values = (user_id,)

    scheduledcomputation_id_tuples: List[tuple] = readDB(scheduledcomputation_prepared_sql, scheduledcomputation_values)
    scheduledcomputation_ids = [id_tuple[0] for id_tuple in scheduledcomputation_id_tuples] # map list of tuples to list of ints

    scheduled_computations = []
    for scheduledcomputation_id in scheduledcomputation_ids:
        scheduled_computations.append(load_scheduled_computation(scheduledcomputation_id))

    return scheduled_computations

def get_user_quota(user_id: str) -> Dict[int, int]:
    url = "http://%s/quota/%s" % (QUOTA_SERVICE_IP, user_id)
    response = requests.get(url=url, headers=headers)
    
    if (response.status_code > 210):
        response_body = response.json()
        print(response_body)
        error_dict = {
        "error": "Error on GET request to quota service", 
        "quota_error_message": response_body.get("detail"), 
        "quota_request": url
        }
        raise HTTPException(status_code=response.status_code, detail=error_dict)

    print(response)
    quota = response.json()
    print("Quota: ", quota)

    return quota

def get_mzn_url(user_id, file_id):
    url = "http://%s/api/minizinc/%s/%s" % (MZN_DATA_SERVICE_IP, user_id, file_id)
    response = requests.get(url, headers=headers)

    if (response.status_code > 210):
        response_body = response.json()
        print(response_body)
        error_dict = {
        "error": "Error on GET request to mzn_data service", 
        "mzn_data_error_message": response_body.get("detail"), 
        "mzn_data_request": url
        }
        raise HTTPException(status_code=response.status_code, detail=error_dict)

    print(response)
    url = response.json()

    print("mzn url")
    print(url)

    return url

def get_solver_image(solver_id):
    url = "http://%s/api/solvers/%s" % (SOLVERS_SERVICE_IP, solver_id)
    response = requests.get("http://%s/api/solvers/%s" % (SOLVERS_SERVICE_IP, solver_id), headers=headers)

    if (response.status_code > 210):
        response_body = response.json()
        print(response_body)
        error_dict = {
        "error": "Error on GET request to solvers service", 
        "solvers_error_message": response_body.get("detail"), 
        "solvers_request": url
        }
        raise HTTPException(status_code=response.status_code, detail=error_dict)

    print(response)
    solver = response.json()
    print("solver image")
    print(solver)  

    solver_image = solver.get("image")
    return solver_image

def get_user_monitor_processes(user_id: str):
    url = "http://%s/api/monitor/processes/%s" % (MONITOR_SERVICE_IP, user_id)
    response = requests.get(url, headers=headers)
    if (response.status_code > 210):
        response_body = response.json()
        print(response_body)
        error_dict = {
        "error": "Error on GET request to monitor service", 
        "monitor_error_message": response_body.get("detail"), 
        "monitor_request": url
        }
        raise HTTPException(status_code=response.status_code, detail=error_dict)

    print("monitor", response)
    user_processes = response.json()
    print("monitor: ", user_processes)
    
    return user_processes

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
