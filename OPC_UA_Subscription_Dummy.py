import asyncio
import logging
import multiprocessing
from asyncua import Client, Node
from asyncua.common.events import Event
import datetime
import os
import sys
import pandas as pd
import re
from multiprocessing import Process, Queue, Value
import time
import sqlalchemy

import SQL_Connection
import ProcessParams


# Logging settings:

log_files = ProcessParams.log_files
filename = datetime.datetime.now().strftime("%Y%m%d") + "_OPCUA_Log.log"
root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.INFO)
asyncua_logger = logging.getLogger("asyncua")
asyncua_logger.setLevel(logging.WARNING)
handler_root = logging.FileHandler(
    filename=log_files + "/ROOT_" + filename, encoding="utf-8", mode="a+"
)
handler_asyncua = logging.FileHandler(
    filename=log_files + "/ASYNCUA_" + filename, encoding="utf-8", mode="a+"
)
formatter = logging.Formatter("%(asctime)s %(name)s:%(levelname)s:%(message)s")
handler_root.setFormatter(formatter)
handler_asyncua.setFormatter(formatter)
root_logger.addHandler(handler_root)
asyncua_logger.addHandler(handler_asyncua)


####################################################################################
# Globals:
####################################################################################

# OPC UA Client
datachange_notification_queue = []
event_notification_queue = []
status_change_notification_queue = []
nodes_to_subscribe = [
    # node-id
    "ns=0;i=2267",
    "ns=0;i=2259",
]
events_to_subscribe = [
    # (eventtype-node-id, event-node-id)
    # ("ns=2;i=1", "ns=2;i=3")
]

users = set()
serverName = "<<ServerName>>"

####################################################################################
# Helper Functions:
####################################################################################


def GetProcessParams(filepath: str) -> dict:
    """
    Reads text file line by line wher each line contains some setting for the script.
    Returns a dictionary with key = variablename and values = variablevalue
    """
    variables = {}
    with open(filepath) as f:
        for line in f:
            name, value = line.split("=")
            variables[name] = value == "True"
    return variables


def storeSQL(df: pd.DataFrame, dirName: str, SQLConnector: SQL_Connection.SQLConnector, cycle: int) -> None:
    """
    Stores dataframe to SQL database using the provided SQLConnector. If the connection fails or there is
    no valid SQLConnector, data is stored as .csv file in the provided directory.
    """
    df_transformed = pd.DataFrame([])
    if set(
        ["PID", "ServerZeit", "ModuleName", "SourceZeit", "NodeName", "NodeValue"]
    ).issubset(df.columns):
        df["Zyklus"] = cycle
        root_logger.info("Table size: %s", df.shape)
        df_transformed = dataTransformation(df)
    if SQLConnector is not None:
        try:
            df_transformed.to_sql(
                ProcessParams.tableName,
                SQLConnector.dbEngine,
                schema="dbo",
                if_exists="append",
                index=False,
            )
            root_logger.info("Data was written to SQL Database.")
        except:
            root_logger.warning(
                "Data could not be written to SQL Database, hence data is stored locally as .csv"
            )
            filename = (
                "{:%Y%m%d_%H%M%S%f}".format(datetime.datetime.now())
                + "_History"
                + ".csv"
            )
            df_transformed.to_csv(dirName + "/" + filename, sep=";")
    else:
        filename = (
            "{:%Y%m%d_%H%M%S%f}".format(datetime.datetime.now()) + "_History" + ".csv"
        )
        df_transformed.to_csv(dirName + "/" + filename, sep=";")


def dataTransformation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Way of transforming data before storing it
    """
    return df


def IsRelevantNode(name: str) -> bool:
    """
    This function filters a node name if it fullfills a predefined name pattern (ensures only relevant nodes are kept)
    """
    return (
        bool(re.search("Fehlermeldung.*_.*\..*-", name))
        | (not bool(re.search("Fehlermeldung", name)))
    ) & (bool(re.search("zeiten.*\[0\]", name)) | (not bool(re.search("zeiten", name))))


async def leaf_collection(root_node: asyncua.Node) -> list:
    """
    Collects all leaf nodes of OPC UA tree, starting from a single root node.
    The leafs are returned as strings of the name of each leaf node. Hence they can be
    called later using root.get_node(<<leafnode_name>>)
    """
    leafs = []
    children = await root_node.get_children()
    while True:
        if len(children) <= 0:
            break
        crt_parent = children.pop(0)
        children_child = await crt_parent.get_children()
        if len(children_child) <= 0:
            relevant = IsRelevantNode(crt_parent.__str__())
            if relevant:
                leafs.append(crt_parent.__str__())
        else:
            children.extend(children_child)
    return leafs


def CollectUpdates(queue: multiprocessing.Queue, prozessmapping: dict, dirName: str, moduleList: list) -> None:
    """
    Function runs on a separate process from all other processes and only collects all node updates
    from the different data subscriptions. Since it runs on separate process, all updates from the
    subscription processes are sent via a multiprocess.Queue and aggregated and transformed with this function.
    """
    i = 0
    array = []
    flag = 0
    root_logger.info("Prozessmapping: %s", prozessmapping)
    crtCycle = dict()
    try:
        ProdDataConnector = SQL_Connection.SQLConnector(
            ProcessParams.database, serverName
        )
        ProdDataConnector.createEngine(False)
    except:
        root_logger.error("Connection to SQL failed")
        ProdDataConnector = None

    messages = {m: [] for m in moduleList}

    while True:
        i += 1
        message = queue.get()
        if message == "Finished":
            flag += 1
            root_logger.info("Finished received from queue (current flag at %s)", flag)
        else:
            moduleName = message["ModuleName"]
            messages[moduleName].append(message)
        if flag >= len(prozessmapping):
            break
        elif (message != "Finished") and re.search(
            "Teil an Folgemodul übergeben", message["NodeName"]
        ):
            crtCycle[moduleName] = message["NodeValue"]
            storeSQL(
                pd.DataFrame(messages[moduleName]),
                dirName,
                ProdDataConnector,
                crtCycle[moduleName],
            )
            messages[moduleName] = []
    for moduleName in moduleList:
        if messages[moduleName]:
            storeSQL(
                pd.DataFrame(messages[moduleName]),
                dirName,
                ProdDataConnector,
                crtCycle[moduleName] + 1,
            )
    root_logger.info("There were a total of %s new datapoints added to SQL", i)

    # Close SQL connection
    if ProdDataConnector.finishSQL():
        root_logger.info("SQL connection was closed succesfully")
    else:
        root_logger.warning("SQL connection could not be closed")


def combineDf(directory: str) -> pd.DataFrame:
    """
    Combining all .csv files in directory into a single dataframe
    """
    filelist = os.listdir(directory)
    df = pd.DataFrame()
    for file in filelist:
        tmp = pd.read_csv(directory + "/" + file, sep=";")
        df = df.append(tmp)
    root_logger.info("Shape of dataframe is %s", df.shape)
    return df


def get_script_path() -> str:
    """
    Return the path of the currently running script
    """
    return os.path.dirname(os.path.realpath(sys.argv[0]))


async def collectRelNodes(moduleUrl: str, moduleName: str, parentNodeNames: list):
    """
    Collects all relevant leaf-nodes from the starting/parent node(s)
    """
    relevant_nodes = []
    vars = []
    try:
        async with Client(url=moduleUrl) as client:
            root_logger.info(
                "Process %s starts collecting its leafs for module %s and parent node %s",
                os.getpid(),
                moduleName,
                parentNodeNames,
            )

            nodes = await client.get_node("ns=3;s=DataBlocksGlobal").get_children()
            for node in nodes:
                if node.__str__() in parentNodeNames:
                    relevant_nodes.append(node)
            for relNode in relevant_nodes:
                leafs = await leaf_collection(relNode)
                vars.extend(leafs)
    except:
        root_logger.warning(
            "Something with the connection went wrong with module %s", moduleName
        )
    return vars


####################################################################################
# Subscription Handler Class:
####################################################################################


class SubscriptionHandler:
    """
    The SubscriptionHandler is used to handle the data that is received for the subscription.
    """

    def __init__(self, queue: multiprocessing.Queue, moduleName: str):
        self.conn = queue
        self.moduleName = moduleName

    def datachange_notification(self, node: asyncua.Node, val: float, data):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        message = {
            "PID": os.getpid(),
            "ServerZeit": data.monitored_item.Value.ServerTimestamp,
            "SourceZeit": data.monitored_item.Value.SourceTimestamp,
            "ModuleName": self.moduleName,
            "NodeName": node.__str__(),
            "NodeValue": val,
            "ReceiveZeit": datetime.datetime.now(),
        }
        self.conn.put(message)
        # datachange_notification_queue.append({"Type": "Datachange Notifcation", "NodeName": node.__str__(), "Value": val, "ServerZeit": data.monitored_item.Value.ServerTimestamp, "SourceZeit": data.monitored_item.Value.SourceTimestamp})

    def event_notification(self, event: Event):
        """
        called for every event notification from server
        """
        event_notification_queue.append(
            {
                "Type": "Event Notifcation",
                "Value": event.get_event_props_as_fields_dict(),
            }
        )

    def status_change_notification(self, status):
        """
        called for every status change notification from server
        """
        status_change_notification_queue.append(status)


####################################################################################
# Main subscription function:
####################################################################################


async def opcua_client(
    moduleName: str, moduleUrl: str, parentNodeNames: str, frequency: list, queue: multiprocessing.Queue, running: bool
) -> None:
    """
    -handles connect/disconnect/reconnect/subscribe/unsubscribe
    -connection-monitoring with cyclic read of the service-level
    """
    client = Client(url=moduleUrl)
    handler = SubscriptionHandler(queue, moduleName)
    subscription = None
    case = 0
    subscription_handle_list = []
    idx = 0
    flag = "run"
    variable_list = []

    while True:
        if not running.value:
            flag = "close"
            case = 4
        if case == 1:
            # connect
            try:
                await client.connect()
                root_logger.info("Connected to module %s!", moduleName)
                case = 2
            except:
                case = 1
                await asyncio.sleep(5)
        elif case == 2:
            # subscribe all nodes and events
            try:
                subscription = await client.create_subscription(frequency[0], handler)
                subscription_handle_list = []
                if nodes_to_subscribe:
                    for node in nodes_to_subscribe + variable_list:
                        handle = await subscription.subscribe_data_change(
                            client.get_node(node)
                        )
                        subscription_handle_list.append(handle)
                if events_to_subscribe:
                    for event in events_to_subscribe:
                        handle = await subscription.subscribe_events(event[0], event[1])
                        subscription_handle_list.append(handle)
                root_logger.info("Subscribed to module %s!", moduleName)
                root_logger.info(
                    "Process %s subscribed to %s nodes", os.getpid(), len(variable_list)
                )
                case = 3
            except Exception as e:
                root_logger.info("Subscription error for modul %s is %s", moduleName, e)
                case = 4
                await asyncio.sleep(0)
        elif case == 3:
            # running => read cyclic the service level if it fails disconnect and unsubscribe => wait 5s => connect
            try:
                if users == set():
                    datachange_notification_queue.clear()
                    event_notification_queue.clear()
                service_level = await client.get_node("ns=0;i=2267").get_value()
                if service_level >= 200:
                    case = 3
                else:
                    root_logger.warning(
                        "Service level of module %s is bad (Level: %s)",
                        moduleName,
                        service_level,
                    )
                    case = 4
                await asyncio.sleep(5)
            except:
                case = 4
        elif case == 4:
            # disconnect clean = unsubscribe, delete subscription then disconnect
            try:
                if subscription_handle_list:
                    for handle in subscription_handle_list:
                        await subscription.unsubscribe(handle)
                if subscription != None:
                    await subscription.delete()
                root_logger.info("Unsubscribed from module %s!", moduleName)
            except Exception as e:
                root_logger.info(
                    "Unsubscribing error for modul %s is %s", moduleName, e
                )
                subscription = None
                subscription_handle_list = []
                await asyncio.sleep(0)
            try:
                await client.disconnect()
                root_logger.info("Disconnected from module %s!", moduleName)
            except Exception as e:
                root_logger.info(
                    "Disconnection error for modul %s is %s", moduleName, e
                )
            variable_list = []
            if flag == "close":
                queue.put("Finished")
                break
            case = 0
        else:
            # wait
            if not variable_list:
                variable_list = await collectRelNodes(
                    moduleUrl, moduleName, parentNodeNames
                )
            case = 1
            await asyncio.sleep(5)


####################################################################################
# Run:
####################################################################################


def dummyFunct(queue: multiprocessing.Queue, nodeNames: list, frequency: list, moduleName: str, moduleUrl: str, running: bool):
    asyncio.run(
        opcua_client(moduleName, moduleUrl, nodeNames, frequency, queue, running)
    )


if __name__ == "__main__":

    # Create directory to store changes of all sensors:
    dir_name = "{:%Y%m%d_%H%M%S}".format(datetime.datetime.now()) + "_History"
    current_directory = get_script_path()
    root_logger.info(f"Working directory is {current_directory}")
    final_directory = os.path.join(current_directory, "Export/" + dir_name)
    if not os.path.exists(final_directory):
        os.makedirs(final_directory)

    processParams_Path = ProcessParams.processParams_Path
    processParams = GetProcessParams(processParams_Path)
    running = Value("i", processParams["running"])

    queue = Queue()
    modulprozesse = []
    for moduleName, moduleUrl in ProcessParams.moduleList.items():
        modulprozesse.append(
            [
                Process(
                    target=dummyFunct,
                    args=(
                        queue,
                        ['ns=3;s="DB_OPC_UA_Fehlermeldung"', 'ns=3;s="DB_OPC_UA"'],
                        [200, 100],
                        moduleName,
                        moduleUrl,
                        running,
                    ),
                ),
                moduleName,
            ]
        )

    prozessmapping = dict()

    for p in modulprozesse:
        p[0].start()
        root_logger.info("Process %s started for module %s", p[0].pid, p[1])
        prozessmapping[p[0].pid] = p[1]

    StoreDataProcess = Process(
        target=CollectUpdates,
        args=(
            queue,
            prozessmapping,
            final_directory,
            list(ProcessParams.moduleList.keys()),
        ),
    )
    StoreDataProcess.start()

    ## Check, if processes should continue to run (or if program should be terminated)
    while running.value:
        processParams = GetProcessParams(processParams_Path)
        running.value = processParams["running"]
        root_logger.info("Current running variable is %s", (running.value == True))
        time.sleep(60)

    for p in modulprozesse:
        p[0].join()

    StoreDataProcess.join()

    if len(os.listdir(final_directory)) > 0:
        # Combine all .csv files to one df
        completeDf = combineDf(final_directory)
        ## Export complete table as .csv
        filename = (
            "{:%Y%m%d_%H%M}".format(datetime.datetime.now())
            + "_CompleteHistory_"
            + moduleName
            + ".csv"
        )
        completeDf.to_csv(final_directory + "/" + filename, sep=";")
    else:
        try:
            os.rmdir(final_directory)
        except OSError as e:
            root_logger.error(
                datetime.datetime.now(),
                ": Error: %s : %s" % (final_directory, e.strerror),
            )
