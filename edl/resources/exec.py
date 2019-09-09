import io
import subprocess
import time

def runyield(cmd, cwd):
    filename    = 'edc.log'
    with io.open(filename, 'wb') as writer, io.open(filename, 'rb', 1) as reader:
        process = subprocess.Popen(cmd, cwd=cwd, shell=True, stdout=writer)
        while process.poll() is None:
            data = reader.read()
            yield reader.read()
            time.sleep(0.1)
        yield reader.read()

