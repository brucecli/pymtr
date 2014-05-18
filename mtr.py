from multiprocessing import Pool, Manager, cpu_count
from Queue import Empty
from subprocess import Popen, PIPE
from datetime import datetime
from json import dumps
import shlex
import pipes
import re

from pandas import DataFrame
from pandas import Timestamp

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

__version_tuple__ = (0,5,0)
__version__ = '.'.join(map(str, __version_tuple__))
__email__ = "mike /at\ pennington [dot] net"
__author__ = "David Michael Pennington <{0}>".format(__email__)
__copyright__ = "{0}, {1}".format(time.strftime('%Y'), __author__)
__license__ = "BSD"
__status__ = "Production"


class MonitorHosts(object):
    def __init__(self, hosts=None, cycles=60, udp=False, timezone="GMT"):
        if not isinstance(hosts, dict):
            raise ValueError, "MonitorHosts() hosts arg must be a dictionary of hosts, example: MonitorHosts(hosts={'host1': '1.1.1.1', 'host2', '1.1.1.2'})"
        self.done = None  # Finished Multiprocessing jobs are stored here
        self.hosts = hosts

        work = list()  # A list of job objects
        for name, host in hosts.items():
            # Build a list of class instances to run()
            work.append(MTR_Job(name=name, host=host, cycles=cycles, 
                udp=udp, timezone=timezone))

        pool = Pool(processes=len(hosts.keys()))
        pool_mgr = Manager()
        q = pool_mgr.Queue()   # Central multiprocessing result queue
        # Multiprocessing for each job.run() is in JobExecQueueMP()
        pool.imap_unordered(JobExecQueueMP, [(job, q) for job in work])
        self.done = self.drain_job_queue(q, work)

        pool.close()
        pool.join()

    def drain_job_queue(self, queue, work):
        done = list()  # A list of jobs done
        # Monitor the multiprocessing queue
        while not (queue.empty()) or (len(done)<len(work)):
            try:
                job = queue.get_nowait()
                done.append(job)
            except Empty:
                pass
            except Exception, e:
                print "Caught", e
                sys.exit(1)
        return done

    def __repr__(self):
        return "<MTR_Collection {0} hosts>".format(len(self.hosts.keys()))

class JobExecQueueMP(object):
    def __init__(self, args):
        """Run under Multiprocessing; test whether the job is valid and 
        run it.  Always add the job to the result queue"""
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
        avg=0.0, worst=0.0, flap=False):
        self.host = host   # Hostname of the mtr target
        self.hop = hop     # Hop number
        self.addr = addr   # address of the hop
        self.sent = int(sent)
        self.drop = int(drop)
        self.best = float(best)
        self.avg = float(avg)
        self.worst = float(worst)
        self.drop_ratio = (self.drop/self.sent)
        self.pct_drop_str = "%0.3f" % (self.drop_ratio*100)
        self.flap = flap   # mtr path flap on this hop

        # I am intentionally leaving drop_ratio out
        self.dictionary = {'host': host,
            'hop': hop,
            'addr': addr,
            'sent': sent,
            'drop': drop,
            'best': best,
            'worst': worst,
            'flap': flap,
            }

    def __repr__(self):
        return "<MTR_Hop {0} to {1}: {2}, {3}% drop, {4} avg>".format(self.hop, 
            self.host, self.addr, self.pct_drop_str, self.avg)

    @property
    def json(self):
        return dumps(self.dictionary)

class MTR_Job(object):
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
        for line in lines:
            mm = rgx_noflap.search(line.strip())
            if not (mm is None):
                hop = MTR_Hop(host=self.host, hop=mm.group(1), addr=mm.group(2),
                    sent=mm.group(3), drop=mm.group(4), best=mm.group(6),
                    avg=mm.group(7), worst=mm.group(8), flap=False)
                self.result.append(hop)  # Add the hop to the list of hops
        return self.result

    def valid(self):
        if isinstance(self.cycles, int):
            self.is_valid = True
            return True
        return False

    def __repr__(self):
        delta = self.end_time - self.start_time
        return "<MTR_Job {0} drop {1}%, ran {2} seconds>".format(self.host, 
            self.result[-1].pct_drop_str, delta.seconds)

if __name__=="__main__":
    jobs = MonitorHosts(hosts={'dns1':"4.2.2.2", 'dns2': "8.8.8.8"}, cycles=5)
    print jobs
    for job in jobs.done:
        print job
