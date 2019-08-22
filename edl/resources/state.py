def update(generator, state_file):
    for item in generator:
        with open(state_file, "a") as f:
            f.write("%s\n" % item)

def new_files(resource_name, state_file, path, ending):
    """Return a list of files that are not present in the state file""" 
    processed_file_set = set()
    logging.info({
        "src":resource_name, 
        "action":"new_%s_files" % ending})
    if os.path.exists(state_file):
        with open(state_file, 'r') as m:
            processed_file_set = set([l.rstrip() for l in m])
    all_file_set = set(glob_dir(path, ending))
    new_file_set = all_file_set - processed_file_set
    logging.info({
        "src":resource_name, 
        "action":"new_%s_files" % ending, 
        "new_file_set_count":len(new_file_set), 
        "all_file_set_count": len(all_file_set), 
        "processed_file_set_count": len(processed_file_set)})
    return list(new_file_set)
