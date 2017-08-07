#! /usr/bin/env python
# -*- encoding: utf-8 -*-
from Constant import *
from pylab import *
import pickle
import pprint


def draw(data):
    ax = subplot(2, 2, 1)
    ax.set_title("Elapsed time (s)")
    plot(nodeArray, data['neo']['t'], color='blue', label='Neo4J')
    plot(nodeArray, data['orient']['t'], color='black', label='OrientDB')
    plot(nodeArray, data['arango']['t'], color='red', label='ArangoDB')

    ax = subplot(2, 2, 2)
    ax.set_title("CPU (%)")
    plot(nodeArray, data['neo']['cpu'], color='blue', label='Neo4J')
    plot(nodeArray, data['orient']['cpu'], color='black', label='OrientDB')
    plot(nodeArray, data['arango']['cpu'], color='red', label='ArangoDB')

    ax = subplot(2, 2, 3)
    ax.set_title("Memory")
    plot(nodeArray, data['neo']['mem'], color='blue', label='Neo4J')
    plot(nodeArray, data['orient']['mem'], color='black', label='OrientDB')
    plot(nodeArray, data['arango']['mem'], color='red', label='ArangoDB')

    ax = subplot(2, 2, 4)
    ax.set_title("Disk")
    plot(nodeArray, data['neo']['disk'], color='blue', label='Neo4J')
    plot(nodeArray, data['orient']['disk'], color='black', label='OrientDB')
    plot(nodeArray, data['arango']['disk'], color='red', label='ArangoDB')

    legend(loc='lower right')
    # savefig("writeV.png")
    show()


def main():
    f = open('writeV.pkl', "rb")
    data = pickle.load(f)
    # pprint.pprint(data)
    draw(data)

if __name__ == '__main__':
    main()
