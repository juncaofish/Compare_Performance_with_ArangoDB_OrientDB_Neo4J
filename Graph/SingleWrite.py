#! /usr/bin/env python
# -*- encoding: utf-8 -*-

from Constant import *
from InitMySQL import *
from watchPerformance import *
from Draw import *
import pyorient
from arango import ArangoClient
import time
import json
import pickle


def writeRecord(id, record, cpu, mem, disk, startTime):
    if id % recordPer == 0:
        calcuteTimeOperate(record, cpu, mem, disk, startTime)
    id += 1
    return id  # no return the id while can't change


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
        session.run("CREATE (a:person {no: '%s', name: '%s'})" % getPersonTuple(
            getSingleInfo(id)))
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
        client.command("insert into persons(no,name) values ('%s','%s')" %
                       getPersonTuple(getSingleInfo(id)))
        id = writeRecord(id, record, cpu, mem, disk, startTime)
    return record, cpu, mem, disk


def ArangoV():
    client = ArangoClient(protocol='http', host='localhost', port=8529, username='root', password='',
                          enable_logging=True)
    db = client.db('graph')
    graphPersons = db.graph('graphPersons')
    persons = graphPersons.vertex_collection("persons")
    id = 1
    record = []
    cpu = []
    mem = []
    disk = []
    startTime = time.time()
    while id <= nodeNum:
        persons.insert(json.loads(json.dumps(
            {'_key': '%s', 'name': '%s'}) % getPersonTuple(getSingleInfo(id))))
        id = writeRecord(id, record, cpu, mem, disk, startTime)
    return record, cpu, mem, disk


def NeoE():
    id = 1
    record = []
    cpu = []
    mem = []
    disk = []
    driver = GraphDatabase.driver(
        "bolt://localhost:7687", auth=basic_auth("neo4j", "welcome"))
    session = driver.session()
    startTime = time.time()
    while id <= relationNum:
        rela_tuple = getRelationTuple(getSingleRelation(id))
        session.run(
            "match (p1:person {no:$p1}),(p2:person {no:$p2}) create (p1)-[:know]->(p2) return p1,p2",
            {"p1": rela_tuple[0], "p2": rela_tuple[1]})
        id = writeRecord(id, record, cpu, mem, disk, startTime)
    return record, cpu, mem, disk


def OriE():
    client = pyorient.OrientDB("localhost", 2424)
    client.connect("root", "welcome")
    client.db_open("graph", "admin", "admin")

    id = 1
    record = []
    cpu = []
    mem = []
    disk = []
    startTime = time.time()
    while id <= relationNum:
        try:
            client.command("create edge from (select from persons where no='%s') to (select from persons where no='%s')" %
                           getRelationTuple(getSingleRelation(id)))
        except:
            id = writeRecord(id, record, cpu, mem, disk, startTime)
            continue
        id = writeRecord(id, record, cpu, mem, disk, startTime)
    return record, cpu, mem, disk


def ArangoE():
    client = ArangoClient(protocol='http', host='localhost', port=8529, username='root', password='',
                          enable_logging=True)
    db = client.db('graph')
    graphPersons = db.graph('graphPersons')
    e = graphPersons.edge_collection('know')

    id = 1
    record = []
    cpu = []
    mem = []
    disk = []
    startTime = time.time()
    while id <= relationNum:
        try:
            e.insert(json.loads(json.dumps(
                {'_from': 'persons/' + '%s', '_to': 'persons/' + '%s'}) % getRelationTuple(getSingleRelation(id))))
        except:
            id = writeRecord(id, record, cpu, mem, disk, startTime)
            continue
        id = writeRecord(id, record, cpu, mem, disk, startTime)
    return record, cpu, mem, disk


def run():

    neoV, neoVCpu, neoVMem, neoVDisk = NeoV()
    oriV, oriVCpu, oriVMem, oriVDisk = OriV()
    araV, araVCpu, araVMem, araVDisk = ArangoV()
    data = {'neo': {'t': neoV, 'cpu': neoVCpu, 'mem': neoVMem, 'disk': neoVDisk},
            'orient': {'t': oriV, 'cpu': oriVCpu, 'mem': oriVMem, 'disk': oriVDisk},
            'arango': {'t': araV, 'cpu': araVCpu, 'mem': araVMem, 'disk': araVDisk}}
    out = open('writeV.pkl', 'wb')
    pickle.dump(data, out)
    out.close()

    neoE, neoECpu, neoEMem, neoEDisk = NeoE()
    oriE, oriECpu, oriEMem, oriEDisk = OriE()
    araE, araECpu, araEMem, araEDisk = ArangoE()
    dataE = {'neo': {'t': neoE, 'cpu': neoECpu, 'mem': neoEMem, 'disk': neoEDisk},
             'orient': {'t': oriE, 'cpu': oriECpu, 'mem': oriEMem, 'disk': oriEDisk},
             'arango': {'t': araE, 'cpu': araECpu, 'mem': araEMem, 'disk': araEDisk}}
    outE = open('writeE.pkl', 'wb')
    pickle.dump(dataE, outE)
    outE.close()
    # drawSingleWriteV(neoV, neoVCpu, neoVMem, neoVDisk, oriV, oriVCpu,
    #                  oriVMem, oriVDisk, araV, araVCpu, araVMem, araVDisk)
    # drawSingleWriteE(neoE, neoECpu, neoEMem, neoEDisk, oriE, oriECpu,
    #                  oriEMem, oriEDisk, araE, araECpu, araEMem, araEDisk)

run()
