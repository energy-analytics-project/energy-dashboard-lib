import os
import logging
import zipfile as zf

def unzip(resource_name, zip_files, input_dir, output_dir):
    """
    Unzip the zip files in input dir to the output directory.

    Return a list of unzipped artifacts that will later be appended
    to the state_file.
    """
    for f in zip_files:
        yield unzip_file(f, resource_name, input_dir, output_dir)

def unzip_file(f, resource_name, input_dir, output_dir):
    try:
        with zf.ZipFile(os.path.join(input_dir, f), 'r') as t:
            for zip_item in t.namelist():
                target_artifact = os.path.join(output_dir, zip_item)
                if not os.path.exists(target_artifact):
                    t.extract(zip_item, output_dir)
                    logging.info({
                        "src":resource_name, 
                        "action":"unzip",
                        "zip_file":f,
                        "zip_item":zip_item,
                        "msg": "item extracted"})
                else:
                    logging.info({
                        "src":resource_name, 
                        "action":"unzip",
                        "zip_file":f,
                        "zip_item":zip_item,
                        "msg": "item skipped (exists already)"})
            return f
    except Exception as e:
        logging.error({
            "src":resource_name, 
            "action":"new_zip_files",
            "file":f,
            "error": e
            })
        return ""
