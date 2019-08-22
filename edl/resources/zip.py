import os
import logging

def new_zip_files(resource_name, state_file, zip_dir):
    """Return a list of zip files that are not present in the state file""" 
    return new_files(resource_name, state_file, zip_dir, "zip")

def unzip(resource_name, zip_files, input_dir, output_dir):
    """
    Unzip the zip files in input dir to the output directory.

    Return a list of unzipped artifacts that will later be appended
    to the state_file.
    """
    unzipped = []
    for f in zip_files:
        try:
            with zf.ZipFile(os.path.join(input_dir, f), 'r') as t:
                for zip_item in t.namelist():
                    target_artifact = os.path.join(output_dir, zip_item)
                    if not os.path.exists(target_artifact):
                        t.extract(zip_item, output_dir)
                        logging.debug({
                            "src":resource_name, 
                            "action":"unzip",
                            "zip_file":f,
                            "zip_item":zip_item,
                            "msg": "item extracted"})
                    else:
                        logging.debug({
                            "src":resource_name, 
                            "action":"unzip",
                            "zip_file":f,
                            "zip_item":zip_item,
                            "msg": "item skipped (exists already)"})
            unzipped.append(f)
        except Exception as e:
            logging.error({
                "src":resource_name, 
                "action":"new_zip_files",
                "file":f,
                "error": e
                })
    return unzipped
