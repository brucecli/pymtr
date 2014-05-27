from multiprocessing import Pool, Manager, cpu_count
from Queue import Empty
from subprocess import Popen, PIPE
from operator import attrgetter
from datetime import datetime
from json import dumps
import shlex
import pipes
import time
import sys
import re

from numpy import int32, float64, nan
from pandas import DataFrame
from pandas import Timestamp
from pandas import HDFStore # Req: libhdf5-dev, cython, numexpr, PyTables
# matplotlib requires:
# - Python libraries
#  - nose
#  - tornado
#  - pyparsing
#
# - System libraries
#  - libfreetype6-dev (freetype2)
#  - libcairo2-dev (libcairo)
#  - libpng
#  - libagg
from matplotlib import pyplot
import matplotlib

"""
Copyright (c) 2014, David Michael Pennington <mike [~at~] pennington dot net>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the <organization> nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

__version_tuple__ = (0,5,1)
__version__ = '.'.join(map(str, __version_tuple__))
__email__ = "mike /at\ pennington [dot] net"
__author__ = "David Michael Pennington <{0}>".format(__email__)
__copyright__ = "{0}, {1}".format(time.strftime('%Y'), __author__)
__license__ = "BSD"
__status__ = "Production"


class MonitorHosts(object):
    def __init__(self, hosts=None, cycles=60, udp=False, 
        timezone="GMT", hdf5_file='store.h5'):
        if not isinstance(hosts, dict):
            raise ValueError, "MonitorHosts() hosts arg must be a dictionary of hosts, example: MonitorHosts(hosts={'host1': '1.1.1.1', 'host2', '1.1.1.2'})"

        self.hosts = hosts
        # Create an empty DataFrame with the correct columns
        datacolumns = ('host', 'hop', 'addr', 'timestamp', 'sent', 'drop', 
            'best', 'worst', 'flap')
        index = ['host', 'hop', 'addr']
        self.df = None

        ## Build an empty result dictionary
        result = dict()
        for col in datacolumns:
            result[col] = list()

        try:
            # Run mtr in an endless loop until the program exits with Cntl-C
            while True:
                jobs = IterRunMP(hosts=hosts, cycles=cycles, udp=udp, 
                    timezone=timezone)

                # Update the result dict with each mtr run...
                for job in jobs.result:
                    # Get each mtr hop result...
                    for row in job.result:
                        # Fill in parameters for each hop
                        for key, val in row.dictionary.items():
                            result[key].append(val)

        except KeyboardInterrupt:
            self.df = DataFrame(result, columns=datacolumns)
            self.df = self.df.set_index(index)
            self.df.sort_index(inplace=True)

            # Store the DataFrame as an HDF5 file...
            hdf = HDFStore(hdf5_file)
            # Append the dataframe, and ensure addr / host can be 17 chars long
            hdf.append('df', self.df, data_columns=list(datacolumns), 
                min_itemsize={'addr': 17, 'host': 17})
            hdf.close()

class IterRunMP(object):
    def __init__(self, hosts=None, cycles=60, udp=False, timezone="GMT"):
        """Build a pool of python processes, and execute a MtrHostJob() in each 
        pool"""
        if not isinstance(hosts, dict):
            raise ValueError, "IterRunMP() hosts arg must be a dictionary of hosts, example: MtrRunMP(hosts={'host1': '1.1.1.1', 'host2', '1.1.1.2'})"
        self.result = None  # Finished Multiprocessing jobs are stored here
        self.hosts = hosts

        work = list()  # A list of job objects
        for name, host in hosts.items():
            # Build a list of class instances to run()
            work.append(MtrHostJob(name=name, host=host, cycles=cycles, 
                udp=udp, timezone=timezone))

        pool = Pool(processes=len(hosts.keys()))
        pool_mgr = Manager()
        q = pool_mgr.Queue()   # Central multiprocessing result queue
        # Multiprocessing for each job.run() is in JobExecQueueMP()
        pool.imap_unordered(JobExecQueueMP, [(job, q) for job in work])
        self.result = self.drain_job_queue(q, work)

        pool.close()
        pool.join()

    def drain_job_queue(self, queue, work):
        result = list()  # A list of jobs done
        # Monitor the multiprocessing queue
        while not (queue.empty()) or (len(result)<len(work)):
            try:
                job = queue.get_nowait()
                result.append(job)
            except Empty:
                pass
            except Exception, e:
                print "Caught", e
                sys.exit(1)
        return result

    def __repr__(self):
        return "<MtrRunMP {0} hosts>".format(len(self.hosts.keys()))

class JobExecQueueMP(object):
    def __init__(self, args):
        """Run a job using Python Multiprocessing; this class spawns itself in 
        a separate python process. Upon execution, test whether the job is 
        valid and run it.  Always add the finished job to the result queue."""
        job, queue = args[0], args[1]
        try:
            if job.valid():
                job.run()
                queue.put(job)
            else:
                queue.put(job)
        except Exception, e:
            job.exception = e
            queue.put(job)

class MTR_Hop(object):
    def __init__(self, host="", hop=0, addr="", sent=0, drop=0, best=0.0, 
        avg=0.0, worst=0.0, timestamp=None, flap=False):
        self.host = host           # host ip address of the end host
        self.hop = int32(hop)      # Hop number
        self.addr = addr           # ip address of the hop
        self.sent = int32(sent)
        self.drop = int32(drop)
        self.best = float64(best)
        self.avg = float64(avg)
        self.worst = float64(worst)
        self.drop_ratio = float64(self.drop/self.sent)
        self.pct_drop_str = "%0.3f" % (self.drop_ratio*100)
        self.timestamp = timestamp # mtr path flap on this hop
        self.flap = flap   # mtr path flap on this hop

    @property
    def dictionary(self):
        # I am intentionally leaving drop_ratio out
        return {'host': self.host,
            'hop': self.hop,
            'addr': self.addr,
            'timestamp': self.timestamp,
            'sent': self.sent,
            'drop': self.drop,
            'best': self.best,
            'worst': self.worst,
            'flap': self.flap,
            }

    @property
    def csv(self):
        return "{0},{1},{2},{3},{4},{5},{6},{7}".format(self.host,
            self.hop, self.addr, self.timestamp, self.sent, self.drop, 
            self.best, self.worst)

    def __repr__(self):
        return "<MTR_Hop {0} to {1}: {2}, {3}% drop, {4} avg>".format(self.hop, 
            self.host, self.addr, self.pct_drop_str, self.avg)

    @property
    def json(self):
        return dumps(self.dictionary)

class MtrHostJob(object):
    def __init__(self, name="", host="", cycles=60, udp=False, timezone="GMT", 
        unittest=""):
        self.name = name
        self.host = host
        self.cycles = cycles
        self.udp = udp
        self.timezone = timezone
        self.unittest = unittest

        self.start_time = Timestamp('now', tz=timezone)  # MTR start time
        self.end_time   = self.start_time                # MTR end time

        self.is_valid = False
        self.exception = ""
        self.result = list()

    def run(self):
        if self.unittest:
            lines = self.unittest.split('\n')
        else:
            options = """-o "SD NBAW" --no-dns --report -c {0}""".format(self.cycles)
            if self.udp:
                options += " -u"

            cmdstr = 'mtr {0} {1}'.format(options, self.host)
            # pipes.quote() is required to escape the quotes in cmdstr
            cmd = Popen(shlex.split(pipes.quote(cmdstr)), shell=True,
                stdout=PIPE)
            lines = cmd.stdout.readlines()
            self.end_time = Timestamp('now', tz=self.timezone)

        # Parse here for precompiled efficiency instead of in MTR_Hop()
        # 3.|-- 99.50.236.2                   5    0    5.2   5.2  11.0  31.3
        rgx_noflap = re.compile(r'\s*(\d+).+?(\d+\.\d+\.\d+\.\d+|\?+)\s+(\d+)\s+(\d+)\s+(\d+\.*\d*)\s+(\d+\.*\d*)\s+(\d+\.*\d*)\s+(\d+\.*\d*)')
        print '\n'.join(lines)
        for line in lines:
            mm = rgx_noflap.search(line.strip())
            if not (mm is None):
                hop = int(mm.group(1))
                addr = mm.group(2)
                sent = int(mm.group(3))
                drop = int(mm.group(4))
                best = mm.group(6)
                avg = mm.group(7)
                worst = mm.group(8)
            else:
                hop = None
                addr = None
                sent = None
                drop = None
                best = None
                avg = None
                worst = None

            if not (hop is None) and (drop<sent):
                hop = MTR_Hop(host=self.host, hop=hop, addr=addr, sent=sent, 
                    drop=drop, best=best, avg=avg, worst=worst, 
                    timestamp=self.start_time, flap=False)
                self.result.append(hop)  # Add the hop to the list of hops
            elif not (hop is None) and (drop==sent):
                hop = MTR_Hop(host=self.host, hop=hop, addr=addr, sent=sent, 
                    drop=drop, best=nan, avg=nan, worst=nan, 
                    timestamp=self.start_time, flap=False)
                self.result.append(hop)  # Add the hop to the list of hops

        return self.result

    def valid(self):
        if isinstance(self.cycles, int):
            self.is_valid = True
            return True
        return False

    @property
    def list_of_dicts(self):
        return [ii.dictionary for ii in self.result]

    @property
    def json_result(self):
        return dumps(self.list_of_dicts)

    def __repr__(self):
        delta = self.end_time - self.start_time
        return "<MtrHostJob {0} drop {1}%, ran {2} seconds>".format(self.host, 
            self.result[-1].pct_drop_str, delta.seconds)

if __name__=="__main__":
    hosts = MonitorHosts(hosts={'dns1':"4.2.2.2", 'dns2': "8.8.8.8", 
        'server': '204.109.61.6'}, 
        timezone="America/Chicago", cycles=10)

    # Find all entries matching host (index) 4.2.2.2 and hop (index) = 1...
    print hosts.df.xs(['4.2.2.2', 1], level=['host', 'hop'])

    # Find all entries matching best<10.0
    print hosts.df['best']<10.0


    # Building a plot with matplotlib
    matplotlib.use('agg')
    pyplot.rc('axes', grid=True)
    pyplot.rc('axes', grid=True)
    pyplot.rc('grid', color='0.75', linestyle='-', linewidth=0.5)
    #textsize = 9
    #left, width = 0.1, 0.8
    #rect1 = [left, 0.7, width, 0.2]
    #rect2 = [left, 0.3, width, 0.4]
    #rect3 = [left, 0.1, width, 0.2]
    #fig = pyplot.figure(facecolor='white')
    #axescolor  = '#f6f6f6'  # the axies background color

    # Plot the average stats for hop 172.16.1.1 across all data points
    data = hosts.df.xs('172.16.1.1', level='addr').groupby('timestamp').mean()
    dates = [str(s) for s in data.index]
    data.plot(lw=4)
    pyplot.savefig('foo.png')


