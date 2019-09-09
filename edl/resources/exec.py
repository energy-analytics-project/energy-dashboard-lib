def runyield(cmd, cwd):
    click.echo("# %s$ %s" % (cwd,cmd))
    filename    = 'edc.log'
    with io.open(filename, 'wb') as writer, io.open(filename, 'rb', 1) as reader:
        process = subprocess.Popen(cmd, cwd=cwd, shell=True, stdout=writer)
        while process.poll() is None:
            data = reader.read()
            if len(data) > 0 and data != "\n":
                yield from reader.read()
            time.sleep(0.1)
        yield from reader.read()

