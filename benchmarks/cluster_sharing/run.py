#!/usr/bin/python
import os
import sys
from subprocess import Popen, PIPE
import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np


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
    # today = datetime.today()
    # print today.strftime(FMT)
    # Get time difference
    time_diff = datetime.strptime(end, FMT) - datetime.strptime(start, FMT)
    # Convert timediff to seconds
    print start, end
    return time_diff.seconds


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
        # self.folder = REPORTS_DIR + '/2016-08-25_14-50-16'
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

        # Plot
        x = ['First Run', 'Second Run']
        y = [first_runtime, second_runtime]
        x_pos = np.arange(len(x))
        # lightskyblue
        plt.barh(x_pos, y, height=0.5, align='center', color='cornflowerblue')
        plt.yticks(x_pos, x)
        plt.xlabel('Seconds')
        plt.title('Runtime Analysis')
        png_path = '{}/runtime.png'.format(self.folder)
        plt.savefig(png_path)
        # plt.show()

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

        # Write to file
        with open('{}/report.markdown'.format(self.folder), 'w') as f:
            f.write(''.join(output))


if __name__ == '__main__':
    # Run cluster benchmark
    mode1 = Mode1(sys.argv[1])
    mode1.run()
    mode1.report()
