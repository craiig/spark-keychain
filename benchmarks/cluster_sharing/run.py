#!/usr/bin/python
import sys
from subprocess import Popen, PIPE


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


def run_cluster_benchmark(benchmark):
    '''
    This:
        - Runs first spark shell
        - Captures BlockManagerMasterURL
        - Runs the benchmark choice until it's done
        - Keeps the shell alive!
        - Runs second spark shell
        - Adds remote Manager Master URL
        - Runs the same benchmark choice until it's done
        - Terminates both spark shells
    '''
    # Run first spark shell
    p1 = Popen(['../../bin/spark-shell'], stdout=PIPE, stdin=PIPE, stderr=PIPE)
    cmd = ['println("BlockManagerURL: " + sc.getBlockManagerMasterURL)',
           ':load {}'.format(benchmark),
           'println("Exit Program")']
    # Send command
    send_command(p1, cmd)
    # Receive outpout
    stdout = receive_output(p1)
    # Extract BlockManagerURL info from stdout
    url = get_block_manager_url(stdout)

    # Run second spark shell
    p2 = Popen(['../../bin/spark-shell'], stdout=PIPE, stdin=PIPE, stderr=PIPE)
    cmd = ['sc.addRemoteBlockManagerMaster("{}")'.format(url),
           ':load {}'.format(benchmark),
           'println("Exit Program")']

    # Send command
    send_command(p2, cmd)
    # Receive outpout
    stdout = receive_output(p2)
    # Extract BlockManagerURL info from stdout


if __name__ == '__main__':
    run_cluster_benchmark(sys.argv[1])
