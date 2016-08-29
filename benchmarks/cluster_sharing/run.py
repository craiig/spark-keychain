#!venv/bin/python
import os
import sys
from subprocess import Popen, PIPE, STDOUT
import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
import pandas as pd
from bokeh.charts import Bar, output_file, show, save
from bokeh.charts.operations import blend


REPORTS_DIR = 'webserver/docs/reports'
SPARK_SHELL = '/home/aziz/spark-1.6.3-SNAPSHOT-bin-craig-sharingONLYLEAF/bin/spark-shell'


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
    '''
    Sends cmd to child process
    '''
    process.stdin.write('\n'.join(cmd)+'\n')
    process.stdin.flush()


def receive_output(process):
    '''
    Prints out the STDOUT of given process,
    and returns a list of the STDOUT lines
    '''
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
    '''
    Returns runtime in seconds
    '''
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
        size_without_unit = float(size_without_unit)
        # Convert size to Bytes
        if unit == 'KB':
            size_without_unit *= 1000
        elif unit == 'MB':
            size_without_unit *= 1000000
        elif unit == 'GB':
            size_without_unit *= 1000000000
        # Put in dictionary
        sizes[rdd_name] = int(size_without_unit)

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


def get_css_style():
    return '''<style> table {
    border-collapse: collapse;
    width: 100%;
    }
    th, td {
    text-align: left;
    padding: 8px;
    }
    tr:nth-child(even){background-color: #f2f2f2}
    </style>'''

def plot_bar(df=None, categories=None, values=None, title=None,
             folder=None, group=None, agg=None):

    kwargs = {}
    if group:
        kwargs['group'] = group
        kwargs['color'] = None
    else:
        kwargs['color'] = 'wheat'

    if agg:
        kwargs['agg'] = agg

    p = Bar(df, categories, values=values, title=title, bar_width=0.5, **kwargs)

    html_file = title.lower().replace(' ', '_')+'.html'
    output_file('{}/{}'.format(folder, html_file))
    save(p)
    return html_file


def get_iframe_tag(html_file):
    iframe_tag = '<iframe src ="../{}" width="800" height="650" frameBorder="0"></iframe>\n'.format(html_file)
    return iframe_tag


class Mode1(object):

    def __init__(self, code_path):
        self.code_path = code_path
        self.timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # Output folder
        self.folder = REPORTS_DIR + '/' + self.timestamp
        # NEXT LINE IS FOR TESTING
        # self.folder = REPORTS_DIR + '/2016-08-27_12-29-44'
        self.folder = REPORTS_DIR + '/2016-08-27_18-40-05'
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
            ['mkdir -p shell1; cd shell1; '+SPARK_SHELL],
            stdout=PIPE,
            stdin=PIPE,
            stderr=STDOUT,
            bufsize=0, shell=True)
        cmd = ['println("BlockManagerURL: " + sc.getBlockManagerMasterURL)',
               ':load ../{}'.format(benchmark),
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
            ['mkdir -p shell2; cd shell2; '+SPARK_SHELL],
            stdout=PIPE,
            stdin=PIPE,
            stderr=STDOUT,
            bufsize=0, shell=True)
        cmd = ['sc.addRemoteBlockManagerMaster("{}")'.format(url),
               ':load ../{}'.format(benchmark),
               'println("Exit Program")']

        # Send command
        send_command(p2, cmd)
        # Receive outpout
        stdout = receive_output(p2)
        # Store stdout
        with open('{}/run2_stdout.txt'.format(self.folder), 'w') as f:
            f.write('\n'.join(stdout))

    def report(self):
        # Headline
        output = [get_css_style() + '\n']
        output.append('# Mode1 Run\n')

        # Timestamp
        output.append('## Timestamp\n- {}\n'.format(self.timestamp))

        # Mechanism
        output.append('## Mechanism\n')
        output.extend(map(lambda x: x.strip() + '\n',
                          self.run.__doc__.split('\n')))
        output.append('\n')

        # Code
        # Get code
        code = get_code(self.code_path)
        # Shift code by two tabs
        code = map(lambda line: '\t\t' + line, code)
        # Report 
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
        # Create Dataframe
        run1_runtime = get_runtime('{}/run1_stdout.txt'.format(self.folder))
        run2_runtime = get_runtime('{}/run2_stdout.txt'.format(self.folder))
        data = {'Run': ['Run 1', 'Run 2'],
                'Time': [run1_runtime, run2_runtime]}
        runs_runtime_df = pd.DataFrame(data)
        # Plot
        runs_runtime_plot = plot_bar(df=runs_runtime_df, categories='Run',
                values='Time', title='Runtime', folder=self.folder)
        # Report 
        output.append('## Runtime\n')
        output.append(get_iframe_tag(runs_runtime_plot))
        output.append(runs_runtime_df.to_html()+'\n')


        # RDD sizes
        # Create Dataframe
        run1_rdd_sizes = get_rdd_sizes('{}/run1_stdout.txt'.format(self.folder))
        run2_rdd_sizes = get_rdd_sizes('{}/run2_stdout.txt'.format(self.folder))
        run1_data = {'Run': 'Run 1',
                     'rdd_name': run1_rdd_sizes.keys(),
                     'sizes': run1_rdd_sizes.values()}
        run2_data = {'Run': 'Run 2',
                     'rdd_name': run2_rdd_sizes.keys(),
                     'sizes': run2_rdd_sizes.values()}
        run1_rdd_sizes_df = pd.DataFrame(run1_data)
        run2_rdd_sizes_df = pd.DataFrame(run2_data)
        runs_rdd_sizes_df = run1_rdd_sizes_df.append(run2_rdd_sizes_df, ignore_index=True)
        # Plot
        runs_rdd_sizes_plot = plot_bar(df=runs_rdd_sizes_df, categories='rdd_name',
                values='sizes', title='RDD sizes', group='Run', folder=self.folder)
        # Report 
        output.append('## RDD Sizes\n')
        output.append(get_iframe_tag(runs_rdd_sizes_plot))
        output.append(runs_rdd_sizes_df.to_html()+'\n')


        # RDD Hits
        # Create Dataframe
        run1_rdd_hits = get_rdd_hits('{}/run1_stdout.txt'.format(self.folder))
        run2_rdd_hits = get_rdd_hits('{}/run2_stdout.txt'.format(self.folder))
        run1_data = {'Run': 'Run 1',
                     'rdd_name': run1_rdd_hits.keys(),
                     'hits': run1_rdd_hits.values()}
        run2_data = {'Run': 'Run 2',
                     'rdd_name': run2_rdd_hits.keys(),
                     'hits': run2_rdd_hits.values()}
        run1_rdd_hits_df = pd.DataFrame(run1_data)
        run2_rdd_hits_df = pd.DataFrame(run2_data)
        runs_rdd_hits_df = run1_rdd_hits_df.append(run2_rdd_hits_df, ignore_index=True)
        # Plot
        runs_rdd_hits_plot = plot_bar(df=runs_rdd_hits_df, categories='rdd_name',
                values='hits', title='RDD Hits', group='Run', folder=self.folder)
        # Report 
        output.append('## RDD Hits\n')
        output.append(get_iframe_tag(runs_rdd_hits_plot))
        output.append(runs_rdd_hits_df.to_html()+'\n')


        # RDD Misses
        # Create Dataframe
        run1_rdd_misses = get_rdd_misses('{}/run1_stdout.txt'.format(self.folder))
        run2_rdd_misses = get_rdd_misses('{}/run2_stdout.txt'.format(self.folder))
        run1_data = {'Run': 'Run 1',
                     'rdd_name': run1_rdd_misses.keys(),
                     'misses': run1_rdd_misses.values()}
        run2_data = {'Run': 'Run 2',
                     'rdd_name': run2_rdd_misses.keys(),
                     'misses': run2_rdd_misses.values()}
        run1_rdd_misses_df = pd.DataFrame(run1_data)
        run2_rdd_misses_df = pd.DataFrame(run2_data)
        runs_rdd_misses_df = run1_rdd_misses_df.append(run2_rdd_misses_df, ignore_index=True)
        # Plot
        runs_rdd_misses_plot = plot_bar(df=runs_rdd_misses_df, categories='rdd_name',
                values='misses', title='RDD misses', group='Run', folder=self.folder)
        # Report 
        output.append('## RDD Misses\n')
        output.append(get_iframe_tag(runs_rdd_misses_plot))
        output.append(runs_rdd_misses_df.to_html()+'\n')

        # RDD Overall Hit/Miss Ratio
        # Create Dataframe
        run1_data = {'Run': 'Run 1',
                     'hits': [sum(run1_rdd_hits.values())],
                     'misses': [sum(run1_rdd_misses.values())],
                     'ratio': [float(sum(run1_rdd_hits.values()))/sum(run1_rdd_misses.values())]}
        run2_data = {'Run': 'Run 2',
                     'hits': [sum(run2_rdd_hits.values())],
                     'misses': [sum(run2_rdd_misses.values())],
                     'ratio': [float(sum(run2_rdd_hits.values()))/sum(run2_rdd_misses.values())]}
        run1_data_df = pd.DataFrame(run1_data)
        run2_data_df = pd.DataFrame(run2_data)
        runs_overall_hit_miss_df = run1_data_df.append(run2_data_df, ignore_index=True)
        # Plot
        runs_overall_hit_miss_plot = plot_bar(df=runs_overall_hit_miss_df, categories='Run',
                values='ratio', title='RDD Overall Hit-Miss Ratio', folder=self.folder)
        # Report 
        output.append('## Overall RDD Hit-Miss Ratio\n')
        output.append(get_iframe_tag(runs_overall_hit_miss_plot))
        output.append(runs_overall_hit_miss_df.to_html()+'\n')


        # Write to file
        with open('{}/report.markdown'.format(self.folder), 'w') as f:
            f.write(''.join(output))


if __name__ == '__main__':
    # Run cluster benchmark
    mode1 = Mode1(sys.argv[1])
    # mode1.run()
    mode1.report()
