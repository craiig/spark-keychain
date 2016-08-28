#!venv/bin/python
import os
import sys
from subprocess import Popen, PIPE
import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
import pandas as pd


REPORTS_DIR = 'webserver/docs/reports'


def get_block_manager_url(stdout):
    '''
    Returns BlockManagerMasterURL from terminal output
    '''
    # Filter
    url = filter(lambda line: line.startswith('BlockManagerURL'), stdout)
    # Parse (expecting only one line with BlockManagerURL)
    url = url[0].split()[-1]
    return url


def send_command(process, cmd):
    process.stdin.write('\n'.join(cmd)+'\n')
    process.stdin.flush()


def receive_output(process):
    stdout = []
    while True:
        output = process.stdout.readline()
        if output == b'':
            raise Exception('Program ended (crashed?) before '
                            'reaching "Exit Program"')
        elif output.strip() == 'Exit Program':
            break
        else:
            stdout.append(output.strip())
            print output.strip()
    return stdout


def get_runtime(filename):
    log = None
    with open(filename) as f:
        log = '\n'.join(f.readlines())
    # Time pattern
    time_pattern = '[0-9]{2}/[0-9]{2}/[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}'
    matches = re.findall(time_pattern, log)
    start, end = matches[0], matches[-1]
    FMT = '%y/%m/%d %H:%M:%S'
    # Get time difference
    time_diff = datetime.strptime(end, FMT) - datetime.strptime(start, FMT)
    # Convert timediff to seconds
    print start, end
    return time_diff.seconds


def get_rdd_sizes(filename):
    '''
    Return a dictionary of
    {rdd_name:rdd_size_in_bytes}
    '''
    log = None
    with open(filename) as f:
        log = '\n'.join(f.readlines())
    # Rdd size pattern
    rdd_size_pattern = 'MemoryStore: Block (rdd_[0-9]+_[0-9]+)\s.*estimated size\s([0-9]*[.][0-9]*\s.*),'
    matches = [m.groups() for m in re.finditer(rdd_size_pattern, log)]
    # Convert all sizes to B
    sizes = {}  # {rdd_name:rdd_size_in_bytes}
    for rdd_name, size in matches:
        size_without_unit, unit = size.split()
        # Convert size to Bytes
        if unit == 'KB':
            size_without_unit *= 1000
        elif unit == 'MB':
            size_without_unit *= 1000000
        elif unit == 'GB':
            size_without_unit *= 1000000000
        # Put in dictionary
        sizes[rdd_name] = int(float(size_without_unit))

    return sizes


def get_rdd_hits(filename):
    '''
    Return a dictionary of
    {rdd_name:hits_for_this_rdd}
    '''
    log = None
    with open(filename) as f:
        log = '\n'.join(f.readlines())
    # Rdd hit pattern
    rdd_hit_pattern = 'BlockManager: Found block (rdd_[0-9]+_[0-9]+)\s'
    matches = [m.groups()[0] for m in re.finditer(rdd_hit_pattern, log)]
    rdd_hits = Counter(matches)
    return rdd_hits


def get_rdd_misses(filename):
    '''
    Return a dictionary of
    {rdd_name:misses_for_this_rdd}
    '''
    log = None
    with open(filename) as f:
        log = '\n'.join(f.readlines())
    # Rdd hit pattern
    rdd_miss_pattern = 'CacheManager: Partition (rdd_[0-9]+_[0-9]+) not found,'
    matches = [m.groups()[0] for m in re.finditer(rdd_miss_pattern, log)]
    rdd_misses = Counter(matches)
    return rdd_misses


def get_code(filename):
    code = None
    with open(filename) as f:
        code = f.readlines()
    return code


class Mode1(object):

    def __init__(self, code_path):
        self.code_path = code_path
        self.timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # Output folder
        self.folder = REPORTS_DIR + '/' + self.timestamp
        # NEXT LINE IS FOR TESTING
        # self.folder = REPORTS_DIR + '/2016-08-27_12-29-44'
        if not os.path.exists(REPORTS_DIR):
            os.makedirs(REPORTS_DIR)
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

    def run(self):
        '''
        - Runs first spark shell
        - Captures BlockManagerMasterURL
        - Runs the benchmark choice until it's done
        - Keeps the shell alive!
        - Runs second spark shell
        - Adds remote Manager Master URL
        - Runs the same benchmark choice until it's done
        - Terminates both spark shells
        '''
        benchmark = self.code_path
        # Run first spark shell
        p1 = Popen(
            ['../../bin/spark-shell'],
            stdout=PIPE,
            stdin=PIPE,
            stderr=PIPE)
        cmd = ['println("BlockManagerURL: " + sc.getBlockManagerMasterURL)',
               ':load {}'.format(benchmark),
               'println("Exit Program")']
        # Send command
        send_command(p1, cmd)
        # Receive outpout
        stdout = receive_output(p1)
        # Store stdout
        with open('{}/run1_stdout.txt'.format(self.folder), 'w') as f:
            f.write('\n'.join(stdout))
        # Extract BlockManagerURL info from stdout
        url = get_block_manager_url(stdout)

        # Run second spark shell
        p2 = Popen(
            ['../../bin/spark-shell'],
            stdout=PIPE,
            stdin=PIPE,
            stderr=PIPE)
        cmd = ['sc.addRemoteBlockManagerMaster("{}")'.format(url),
               ':load {}'.format(benchmark),
               'println("Exit Program")']

        # Send command
        send_command(p2, cmd)
        # Receive outpout
        stdout = receive_output(p2)
        # Store stdout
        with open('{}/run2_stdout.txt'.format(self.folder), 'w') as f:
            f.write('\n'.join(stdout))

    def report(self):
        # Example: runtime
        first_runtime = get_runtime('{}/run1_stdout.txt'.format(self.folder))
        second_runtime = get_runtime('{}/run2_stdout.txt'.format(self.folder))
        print 'First runtime:\t', first_runtime
        print 'Second runtime:\t', second_runtime

        # Runtime Plot
        x = ['First Run', 'Second Run']
        y = [first_runtime, second_runtime]
        x_pos = np.arange(len(x))
        # lightskyblue
        plt.barh(x_pos, y, align='center', color='cornflowerblue')
        plt.yticks(x_pos, x)
        plt.xlabel('Seconds')
        plt.title('Runtime Analysis')
        png_path = '{}/runtime.png'.format(self.folder)
        plt.tight_layout()
        plt.axis('tight')
        plt.savefig(png_path)
        plt.close()

        # RDD Sizes plot
        plt.figure(1, [10, 30])
        rdd_sizes = get_rdd_sizes('{}/run1_stdout.txt'.format(self.folder))
        x = rdd_sizes.keys()
        y = rdd_sizes.values()
        x_pos = np.arange(len(x))
        # lightskyblue
        plt.barh(x_pos, y, align='center', color='cornflowerblue')
        plt.yticks(x_pos, x)
        plt.xlabel('Bytes')
        plt.title('RDD Sizes')
        png_path = '{}/rdd_sizes.png'.format(self.folder)
        plt.tight_layout()
        plt.axis('tight')
        plt.savefig(png_path)
        plt.close()

        # RDD Hits
        plt.figure(1, [10, 30])
        rdd_hits = get_rdd_hits('{}/run1_stdout.txt'.format(self.folder))
        run1_rdd_hits = get_rdd_hits('{}/run1_stdout.txt'.format(self.folder))
        run2_rdd_hits = get_rdd_hits('{}/run2_stdout.txt'.format(self.folder))
        x = rdd_hits.keys()
        y = rdd_hits.values()
        x_pos = np.arange(len(x))
        # lightskyblue
        plt.barh(x_pos, y, align='center', color='cornflowerblue')
        plt.yticks(x_pos, x)
        plt.xlabel('Count')
        plt.title('RDD Hits')
        png_path = '{}/rdd_hits.png'.format(self.folder)
        plt.tight_layout()
        plt.axis('tight')
        plt.savefig(png_path)
        plt.close()

        # RDD Hits (table)
        raw_data = {'rdd_name': rdd_hits.keys(),
                    'Hits': rdd_hits.values()}
        df = pd.DataFrame(raw_data)
        df = df.set_index('rdd_name')
        rdd_hits_html = df.to_html()

        # RDD Misses
        plt.figure(1, [10, 30])
        rdd_misses = get_rdd_misses('{}/run1_stdout.txt'.format(self.folder))
        run1_rdd_misses = get_rdd_misses('{}/run1_stdout.txt'.format(self.folder))
        run2_rdd_misses = get_rdd_misses('{}/run2_stdout.txt'.format(self.folder))
        x = rdd_misses.keys()
        y = rdd_misses.values()
        x_pos = np.arange(len(x))
        # lightskyblue
        plt.barh(x_pos, y, align='center', color='cornflowerblue')
        plt.yticks(x_pos, x)
        plt.xlabel('Count')
        plt.title('RDD Misses')
        png_path = '{}/rdd_misses.png'.format(self.folder)
        plt.tight_layout()
        plt.axis('tight')
        plt.savefig(png_path)
        plt.close()

        # RDD Overall Hit/Miss ratio
        plt.figure(1)
        run1_hits = sum(run1_rdd_hits.values())
        run2_hits = sum(run2_rdd_hits.values())
        run1_misses = sum(run1_rdd_misses.values())
        run2_misses = sum(run2_rdd_misses.values())
        x = ['Hit', 'Miss']
        y = [run1_hits, run1_misses]
        pos = np.arange(len(x))
        width = 0.25
        # lightskyblue
        ticks = [p + width for p in pos]
        plt.barh(ticks, y, height=0.25, align='center', color='cornflowerblue')
        plt.yticks(ticks, x)

        y = [run2_hits, run2_misses]
        ticks = [p + width*2 for p in pos]
        plt.barh(ticks, y, height=0.25, align='center', color='cornflowerblue')

        plt.xlabel('Count')
        #plt.title('RDD Overall Hits/Misses (Ratio= {})'.format(float(hits)/misses))
        png_path = '{}/rdd_overall_hit_miss_ratio.png'.format(self.folder)
        plt.tight_layout()
        plt.axis('tight')
        plt.savefig(png_path)
        plt.close()

        y = [run1_hits, run1_misses]
        print y
        y = [run2_hits, run2_misses]
        print y
        # Get code
        code = get_code(self.code_path)
        # Shift code by two tabs
        code = map(lambda line: '\t\t' + line, code)

        # Headline
        output = []
        output.append('# Mode1 Run\n')

        # Timestamp
        output.append('## Timestamp\n- {}\n'.format(self.timestamp))

        # Mechanism
        output.append('## Mechanism\n')
        output.extend(map(lambda x: x.strip() + '\n',
                          self.run.__doc__.split('\n')))
        output.append('\n')

        # Code
        output.append('## Code\n')
        output.append('- This test runs **{}**:\n\n'.format(
            self.code_path.split('/')[-1]))
        output.extend(code)
        output.append('\n')

        # Logs
        output.append('## Logs\n')
        output.append('- [First Run Stdout](run1_stdout.txt)\n')
        output.append('- [Second Run Stdout](run2_stdout.txt)\n')

        # Runtime
        output.append('## Runtime Analysis\n')
        output.append('![](runtime.png)\n')

        # RDD Sizes
        output.append('## RDD Sizes\n')
        output.append('- RDD sizes of first run.\n\n')
        output.append('![](rdd_sizes.png)\n')

        # RDD Hits
        output.append('## RDD Hits\n')
        output.append('![](rdd_hits.png)\n')
        output.append('\n')
        output.append(rdd_hits_html)
        output.append('\n')
        output.append(df.describe().to_html())
        output.append('\n')

        # RDD Misses
        output.append('## RDD Misses\n')
        output.append('![](rdd_misses.png)\n')

        # Overall RDD Hit/Miss Ratio
        output.append('## Overall RDD Hit/Miss Ratio\n')
        output.append('![](rdd_overall_hit_miss_ratio.png)\n')

       # Write to file
        with open('{}/report.markdown'.format(self.folder), 'w') as f:
            f.write(''.join(output))


if __name__ == '__main__':
    # Run cluster benchmark
    mode1 = Mode1(sys.argv[1])
    mode1.run()
    mode1.report()
