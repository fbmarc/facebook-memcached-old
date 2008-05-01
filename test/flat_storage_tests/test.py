#!/usr/bin/env python2.5

import optparse
import os
import subprocess
import sys
from test_suites import test_suites


def which(filename):
    import os

    if (not os.environ.has_key('PATH') or os.environ['PATH'] == ''):
        p = os.defpath
    else:
        p = os.environ['PATH']
        
    pathlist = p.split(os.pathsep)
    
    for path in pathlist:
        f = os.path.join(path, filename)
        if os.access(f, os.X_OK):
            return f
    return None


def get_script_location():
    global script_location
    
    parent_dir = os.path.split(sys.argv[0])[0]
    script_location = os.path.join(os.getcwd(), parent_dir)


def do_shell_exec(cmd):
    exe = os.path.normpath(os.path.join(script_location, cmd))
    args = [cmd]
    if (opts.verbose):
        args.append(str(opts.verbose))
    if (opts.fast):
        args.append("1")

    po = subprocess.Popen(args, executable = exe)

    retcode = po.wait()
    assert (retcode == 0)


def do_scons(unoptimized, targets):
    exe = which("scons")
    args = [os.path.split(exe)[1], "-C", script_location]
    if (unoptimized):
        args.append("debug=1")
    args.extend(targets)

    po = subprocess.Popen(args, executable = exe)

    retcode = po.wait()
    assert (retcode == 0)


def main():
    get_script_location()
    
    global opts
    
    parser = optparse.OptionParser()
    parser.add_option("-a", "--all", action = "store_true",
                      help = "Run all tests")
    parser.add_option("--verbose", action = "store", type = "int", dest = "verbose",
                      help = "verbose")
    parser.add_option("--fast", action = "store_true", dest = "fast",
                      help = "run only fast tests")
    parser.add_option("--debug", action = "store_true", dest = "unoptimized",
                      help = "unoptimized binaries")
    parser.set_defaults(verbose = 0)

    tests = list()
    
    for test_suite in test_suites:
        func_opt = test_suite['binary_name'].replace("_", "-")

        parser.add_option("--%s" % func_opt, action = "store_true",
                          help = "Run %s" % test_suite['binary_name'])
        
    opts, args = parser.parse_args()

    for test_suite in test_suites:
        if (getattr(opts, test_suite['binary_name'], None)):
            tests.append( test_suite )

    if (len(tests) == 0):
        tests = test_suites[:]

    do_scons(opts.unoptimized, [test_suite['binary_name']
                                for test_suite in tests])
    print
    for test_suite in tests:
        func, func_args = test_suite['runtime_info']
        print("Running test %s" % test_suite['binary_name'])
        if (func in globals()):
            globals()[func](*func_args)


if __name__ == '__main__':
    main_thread_alive = True
    try:
        main()
    except:
        main_thread_alive = False
        raise
