#!/usr/bin/env python2.5

import optparse
import os
import random
import re
import select
import signal
import socket
import subprocess
import sys
import threading
import time

import pexpect

import mcc


ALPHANUM = 'abcdefghijklmnopqrstuvwxyz0123456789aBCDEFGHIJKLMNOPQRSTUVWXYZ'
def newTestStrings(count=100, min=8, max=16, alphabet=ALPHANUM):

    result = []

    for ix in range(count):
	length = random.randint(min, max)

	s = ''

	for jx in range(length):
	    x = random.randint(0, len(alphabet) - 1)
	    if x >= len(alphabet):
		raise Exception, str(x)
	    s+= alphabet[x]
	result.append(s)
    return result


def assert_keys(mc, kvs, debugp=None):
    result = mc.get(*kvs.keys())
    nullValues = len([v for v in kvs.values() if v == None])
    assert len(result) == len(kvs)

    if nullValues == 0:
        errors = mc.errors();
        debugp and errors and debugp('\t' + '\n\t'.join([str(e) for e in errors]))
        assert mc.errors() == None
    else:
        mc.errors()

    for k,v in result.items():
	assert kvs.get(k) == v


def get(mc, key):
    result = mc.get(key)
    assert (key in result)
    return result[key]


object_list_updated = False
objects_to_poll = dict()


def generic_dumper():
    global main_thread_alive

    while (main_thread_alive):
        pobj = select.poll()
        fd_name_map = dict()
        for mcd_name, (mcd_obj, process) in objects_to_poll.iteritems():
            if (mcd_obj.process is not None):
                pobj.register(mcd_obj.process.stderr, select.POLLIN)
                fd_name_map[mcd_obj.process.stderr.fileno()] = mcd_name
            else:
                del objects_to_poll[mcd_name]

        ready = pobj.poll(1000)

        for fd, event in ready:
            mcd_obj, process = objects_to_poll[fd_name_map[fd]]

            # the mcd_obj.process may have been obliterated.  so use the direct process
            # object instead.

            if (event & select.POLLIN):
                inp = process.stderr.readline()
                if (len(inp) != 0):
                    sys.stderr.write("%s: %s" % (mcd_obj.name, inp))

            if (process.poll() is not None):
                # process has died.  remove from list we monitor and continue.
                del objects_to_poll[mcd_obj.name]

        if (len(objects_to_poll) == 0):
            break


def defaultProgram(base):

    name = os.path.join(os.environ.get('HOME'), 'bin', base)

    if os.path.exists(name):
	return name

    names = [ os.path.join(p, base) for p in os.environ['PATH'].split(os.path.pathsep) if os.path.exists(os.path.join(p, base))]

    if len(names) > 0:
	return names[0]
    else:
	raise Exception, 'Can\'t fine '+base+' in '+os.environ['PATH']


class Memcached(object):
    # if port = None, then try launching memcached until we don't get a bind() error.  if
    # useUdp is True and udpPort is None, do the same for the udp port.
    #
    # if udpPort is set, then useUdp is ignored.
    def __init__(self, name="memcached", port=None, useUdp=False, udpPort=None,
                 program=defaultProgram('memcached'), extra_args = []):
	self.name = name
	self.port = port
        self.useUdp = (udpPort is not None) or useUdp;
	self.udpPort = udpPort
	self.program = program
	self.args = [os.path.split(self.program)[-1]]
	self.process = None
        self.running = False
        self.extra_args = extra_args

    def start(self):

        if self.running:
            return
        # divine which items we're going to autoconfig (and possibly retry).
        autoPortConfig = True
        autoUdpPortConfig = False

        if (self.port is not None):
            autoPortConfig = False
            self.args.extend(["-p", str(self.port)])

        if (self.useUdp):
            if (self.udpPort is not None):
                self.args.extend(["-U", str(self.udpPort)])
            else:
                autoUdpPortConfig = True

        autoConfig = autoPortConfig or autoUdpPortConfig

        while (True):
            args = self.args[:]
            if (autoPortConfig):
                # randomly assign it a port
                self.port = random.randint(0, 65536)
                args.extend(["-p", str(self.port)])
            if (autoUdpPortConfig):
                if (autoPortConfig):
                    # use the same port as TCP.
                    self.udpPort = self.port
                else:
                    # randomly assign it a port
                    self.udpPort = random.randint(0, 65536)
                args.extend(["-U", str(self.udpPort)])

            if (globals().has_key("memcached_verbose")):
                args.append("-vv")

            args.extend(self.extra_args)

            if (not autoConfig):
                self.process = subprocess.Popen(args, 0, self.program)
                break

            self.process = subprocess.Popen(args, 0, self.program,
                                            stderr=subprocess.PIPE)

            # bind failure?
            pobj = select.poll()

            pobj.register(self.process.stderr, select.POLLIN)
            pret = pobj.poll(1000)
            if (len(pret) == 0):
                # no events waiting.  this probably means we're good to go.  copy the args
                # so we're consistent with previous behavior.
                self.args = args[:]
                return

            line = self.process.stderr.readline()
            if (line.startswith("bind()")):
                # bind error.  retry....
                continue
            sys.stderr.write("%s: %s" % (self.name, line))

            self.args = args[:]
            break

        # we have a process now, dump the stderr to stderr periodically.
        if (autoConfig):
            if (len(objects_to_poll) == 0):
                objects_to_poll[self.name] = (self, self.process)
                self.dumper_thread = threading.Thread(target = generic_dumper)
                self.dumper_thread.start()
            else:
                objects_to_poll[self.name] = self
                object_list_updated = True

        self.running = True


    def stop(self):
	if not self.process:
	    return
	os.kill(self.process.pid, signal.SIGHUP)
	self.process = None
        self.running = False

    def __del__(self):
	self.stop()


def get_sideband_mc_connection(server, port):
    pexp = pexpect.spawn("/bin/sh")
    pexp.sendline("\n\n")
    pexp.expect("[$%#>]")
    pexp.sendline("telnet %s %d" % (server, port))
    pexp.expect("Connected to ")

    return pexp


def get_metainfo(pexp, key):
    pexp.sendline("metaget %s" % key)
    rexp = ("(META " + key + " age: (?P<age>\\d+); "
            "exptime: (?P<exptime>\\d+); "
            "from: (?P<ip>\\S+))?\r\nEND\r\n")
    pexp.expect(rexp)
    groups = pexp.match.groups()
    if (groups[0] is not None):
        return (int(groups[1]), int(groups[2]), groups[3])
    else:
        return None


def kill_sideband_mc_connection(pexp):
    pexp.kill(signal.SIGINT)


def set_test():
    md = Memcached()
    md.start()
    mc = mcc.MCC(name="default")
    mc.add_serverpool("wildcard")
    mc.default_serverpool = "wildcard"
    server_name = "%s:%d" % ("127.0.0.1", md.port)
    mc.add_server(server_name)
    mc.add_accesspoint(server_name, "127.0.0.1", md.port)
    mc.serverpool_add_server("wildcard", server_name)

    pexp = get_sideband_mc_connection("127.0.0.1", md.port)

    k, v = newTestStrings(2)
    mc.set(k, v)

    age, exptime, ip = get_metainfo(pexp, k)
    assert(age <= 1)
    assert(exptime == 0)
    assert(ip == "unknown" or ip == "127.0.0.1")

    kill_sideband_mc_connection(pexp)
    del mc
    md.stop()
    del md


def not_present_test():
    md = Memcached()
    md.start()
    mc = mcc.MCC(name="default")
    mc.add_serverpool("wildcard")
    mc.default_serverpool = "wildcard"
    server_name = "%s:%d" % ("127.0.0.1", md.port)
    mc.add_server(server_name)
    mc.add_accesspoint(server_name, "127.0.0.1", md.port)
    mc.serverpool_add_server("wildcard", server_name)

    pexp = get_sideband_mc_connection("127.0.0.1", md.port)

    k, v = newTestStrings(2)
    mc.set(k, v)

    result = get_metainfo(pexp, "%s1" % k)

    kill_sideband_mc_connection(pexp)
    del mc
    md.stop()
    del md


def arith_test():
    md = Memcached()
    md.start()
    mc = mcc.MCC(name="default")
    mc.add_serverpool("wildcard")
    mc.default_serverpool = "wildcard"
    server_name = "%s:%d" % ("127.0.0.1", md.port)
    mc.add_server(server_name)
    mc.add_accesspoint(server_name, "127.0.0.1", md.port)
    mc.serverpool_add_server("wildcard", server_name)

    pexp = get_sideband_mc_connection("127.0.0.1", md.port)

    k = newTestStrings(1)[0]
    mc.set(k, "1")

    time.sleep(5)

    mc.incr(k, 15)

    age, exptime, ip = get_metainfo(pexp, k)
    assert(age <= 1)
    assert(exptime == 0)
    assert(ip == "unknown" or ip == "127.0.0.1")

    kill_sideband_mc_connection(pexp)
    del mc
    md.stop()
    del md


def set_with_exptime_test():
    md = Memcached()
    md.start()
    mc = mcc.MCC(name="default")
    mc.add_serverpool("wildcard")
    mc.default_serverpool = "wildcard"
    server_name = "%s:%d" % ("127.0.0.1", md.port)
    mc.add_server(server_name)
    mc.add_accesspoint(server_name, "127.0.0.1", md.port)
    mc.serverpool_add_server("wildcard", server_name)

    pexp = get_sideband_mc_connection("127.0.0.1", md.port)

    k, v = newTestStrings(2)
    mc.set(k, v, 15)

    age, exptime, ip = get_metainfo(pexp, k)
    assert(age <= 1)
    assert(exptime >= 15)
    assert(ip == "unknown" or ip == "127.0.0.1")

    kill_sideband_mc_connection(pexp)
    del mc
    md.stop()
    del md


def main():
    global opts

    parser = optparse.OptionParser()
    parser.add_option("-a", "--all", action = "store_true",
                      help = "Run all tests")
    parser.add_option("--gdb-friendly", action = "store_true", dest = "gdb_friendly",
                      help = "gdb-friendly mode, i.e., long timeout")
    parser.add_option("--dump-stats", action="store_true", dest = "dump_stats",
                      help = "dump stats as read from the server")
    parser.add_option("--mcd-verbose", action="store_true",
                      help = "enable memcached -vv mode")

    all_tests_funcs = [
        set_test,
        not_present_test,
        arith_test,
        set_with_exptime_test,
    ]

    tests = list()

    for func in all_tests_funcs:
        func_opt = func.func_name.replace("_", "-")

        parser.add_option("--%s" % func_opt, action = "store_true",
                          help = "Run %s" % func.func_name)

    opts, args = parser.parse_args()

    for func in all_tests_funcs:
        if (getattr(opts, func.func_name, None)):
            tests.append( func )

    if (len(tests) == 0):
        tests = all_tests_funcs[:]

    if (opts.mcd_verbose):
        global memcached_verbose
        memcached_verbose = True

    for func in tests:
        print("Running test %s"  % func.func_name)
        func()


if __name__ == '__main__':
    main_thread_alive = True
    try:
        main()
    except:
        main_thread_alive = False
        raise
