#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import urllib
import urllib2
from optparse import OptionParser

parser = OptionParser()

parser.add_option("-a", "--add_servers", dest="add_servers",
                  help="add servers to the cluster", metavar="COMMA_SEPARATED_SERVER_ADDRESS")
parser.add_option("-d", "--del_servers", dest="del_servers",
                  help="remove servers from the cluster", metavar="COMMA_SEPARATED_SERVER_ADDRESS")
parser.add_option("-l", "--leader_address", dest="leader_address",
                  help="leader address, default to localhost", metavar="LEADER_SERVER_ADDRESS", default="localhost")
parser.add_option("-t", "--target_server", dest="target_server",
                  help="transfer leadership to the target server", metavar="TARGET_SERVER_ADDRESS")
parser.add_option("-s", "--show_servers", dest="show_servers", action="store_true",
                  help="show servers in the cluster")

(options, args) = parser.parse_args()

if (options.add_servers == None):
    options.add_servers = []
else:
    options.add_servers = options.add_servers.split(",")

if (options.del_servers == None):
    options.del_servers = []
else:
    options.del_servers = options.del_servers.split(",")

leader_address = options.leader_address
add_servers = options.add_servers
del_servers = options.del_servers

if (options.show_servers):
    req = urllib2.Request("http://" + leader_address + ":29998/list_server")
    try:
        resp = urllib2.urlopen(req)
        data = resp.read()
        print "server address list: {0}\n".format(data)
    except Exception as e:
        print "error requesting leader {0}: {1}".format(leader_address, e)

if (options.target_server != None):
    req = urllib2.Request("http://" + leader_address + ":29998/leader_transfer?server=" + options.target_server)
    print "request the current leader[{0}] to transfer leadership to server {1}".format(leader_address, options.target_server)
    try:
        resp = urllib2.urlopen(req)
        data = resp.read()
        print data
    except Exception as e:
        print "error requesting leader {0}: {1}".format(leader_address, e)

for a in add_servers:
    while True:
        req = urllib2.Request("http://" + leader_address + ":29998/add_server?server=" + a)
        print "request the leader[{0}] to add server {1}".format(leader_address, a)
        try:
            resp = urllib2.urlopen(req)
            data = resp.read()
            (status, leader_hint) = data.split(" ")
            if (status == "OK"):
                print "succeed"
            else:
                print "failed to add server {0}: {1} current leader: {2}".format(a, status, leader_hint)
            cur_leader = leader_hint.split(":")[0]
            if (cur_leader != leader_address):
                print "leader changed to {0}, retry...".format(cur_leader)
                leader_address = cur_leader
            else:
                break
        except Exception as e:
            print "error requesting leader {0}: {1}".format(leader_address, e)
            break

for a in del_servers:
    while True:
        req = urllib2.Request("http://" + leader_address + ":29998/remove_server?server=" + a)
        print "request the leader[{0}] to delete server {1}".format(leader_address, a)
        try:
            resp = urllib2.urlopen(req)
            data = resp.read()
            (status, leader_hint) = data.split(" ")
            if (status == "OK"):
                print "succeed"
            else:
                print "failed to remove server {0}: {1} current leader: {2}".format(a, status, leader_hint)
            cur_leader = leader_hint.split(":")[0]
            if (cur_leader != leader_address):
                print "leader changed to {0}, retry...".format(cur_leader)
                leader_address = cur_leader
            else:
                break
        except Exception as e:
            print "error requesting leader {0}: {1}".format(leader_address, e)
            break