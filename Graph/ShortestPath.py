from Constant import *
from InitMySQL import *
from watchPerformance import *
from Draw import *
import pyorient
from arango import ArangoClient
import time
import random


def writeRecord(id, record, cpu, mem, disk, startTime):
    if id % recordPer == 0:
        calcuteTimeOperate(record, cpu, mem, disk, startTime)
    id += 1
    return id  # no return the id while can't change


def getRamdomNo():
    return getNumStr(random.randint(0, nodeNum))


def NeoV():
    id = 1
    record = []
    cpu = []
    mem = []
    disk = []
    driver = GraphDatabase.driver(
        "bolt://localhost:7687", auth=basic_auth("neo4j", "welcome"))
    session = driver.session()
    startTime = time.time()
    while id <= nodeNum:
        a = []
        a.append(getRamdomNo())
        a.append(getRamdomNo())
        try:
            session.run(
                "match (p1:person{no:$p1}),(p2:person{no:$p2}) match p=shortestPath((p1)-[*..3]->(p2)) return p", {"p1": a[0], "p2": a[1]})
        except:
            id = writeRecord(id, record, cpu, mem, disk, startTime)
            continue
        id = writeRecord(id, record, cpu, mem, disk, startTime)
    return record, cpu, mem, disk


def OriV():
    client = pyorient.OrientDB("localhost", 2424)
    client.connect("root", "welcome")
    client.db_open("graph", "admin", "admin")

    id = 1
    record = []
    cpu = []
    mem = []
    disk = []
    startTime = time.time()
    while id <= nodeNum:
        a = []
        a.append(getRamdomNo())
        a.append(getRamdomNo())
        try:
            client.command(
                "select dijkstra((select @RID from persons where no='%s'),(select @RID from persons where no='%s'),'E')" % tuple(a))
        except:
            id = writeRecord(id, record, cpu, mem, disk, startTime)
            continue
        id = writeRecord(id, record, cpu, mem, disk, startTime)
    return record, cpu, mem, disk


def ArangoV():
    client = ArangoClient(protocol='http', host='localhost', port=8529, username='root', password='',
                          enable_logging=True)
    db = client.db('graph')
    graphPersons = db.graph('graphPersons')
    id = 1
    record = []
    cpu = []
    mem = []
    disk = []
    startTime = time.time()
    while id <= nodeNum:
        a = []
        a.append('persons' + getRamdomNo())
        a.append('persons' + getRamdomNo())
        try:
            db.aql.execute(
                "for v,e in outbound shortest_path '%s' to '%s' graph 'graphPersons' return [v._key,e._key]" % tuple(a))
        except:
            id = writeRecord(id, record, cpu, mem, disk, startTime)
            continue
        id = writeRecord(id, record, cpu, mem, disk, startTime)
    return record, cpu, mem, disk


def run():
    import pickle
    neoV, neoVCpu, neoVMem, neoVDisk = NeoV()
    oriV, oriVCpu, oriVMem, oriVDisk = OriV()
    araV, araVCpu, araVMem, araVDisk = ArangoV()
    data = {'neo': {'t': neoV, 'cpu': neoVCpu, 'mem': neoVMem, 'disk': neoVDisk},
            'orient': {'t': oriV, 'cpu': oriVCpu, 'mem': oriVMem, 'disk': oriVDisk},
            'arango': {'t': araV, 'cpu': araVCpu, 'mem': araVMem, 'disk': araVDisk}}
    out = open('ShortedPath.pkl', 'wb')
    pickle.dump(data, out)
    out.close()

    drawSingleWriteV(neoV, neoVCpu, neoVMem, neoVDisk, oriV, oriVCpu,
                     oriVMem, oriVDisk, araV, araVCpu, araVMem, araVDisk)

run()
