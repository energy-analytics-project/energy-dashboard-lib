import os
import logging
"""
rclone notes:

    'rclone sync' is a destructive command that not only copies files to the
    target but also deletes extra files on the target, leaving the target as an
    exact copy of the source. This was very useful to use while the structure
    of the files on disk was changing, especially as I was running into issues
    with file name conventions, number of directories/buckets allowed in a
    space (100 on DO), etc. Now that this has stabilized, sync is deprecated in
    favor of copy.

    For example, when I have run this manually:

        $ cd eap
        $ ls
            data-source-template
            energy-dashboard
        $ rclone sync -v --include='*.zip' eap/ eap:eap

    This results in:

       63 data-oasis-as-mileage-calc-all/.git
       90 data-oasis-as-mileage-calc-all/.gitattributes
     1348 data-oasis-as-mileage-calc-all/.gitignore
    11405 data-oasis-as-mileage-calc-all/LICENSE
      920 data-oasis-as-mileage-calc-all/Makefile
      596 data-oasis-as-mileage-calc-all/README.md
      133 data-oasis-as-mileage-calc-all/db/data-oasis-as-mileage-calc-all.db
   146949 data-oasis-as-mileage-calc-all/db/inserted.txt
      329 data-oasis-as-mileage-calc-all/manifest.json
     4959 data-oasis-as-mileage-calc-all/src/10_down.py
     3140 data-oasis-as-mileage-calc-all/src/20_unzp.py
    11104 data-oasis-as-mileage-calc-all/src/30_inse.py
       82 data-oasis-as-mileage-calc-all/src/40_save.sh
      128 data-oasis-as-mileage-calc-all/xml/20121231_20130101_AS_MILEAGE_CALC_N_20190806_08_20_22_v1.xml
      128 data-oasis-as-mileage-calc-all/xml/20130101_20130102_AS_MILEAGE_CALC_N_20190806_08_20_28_v1.xml
      128 data-oasis-as-mileage-calc-all/xml/20130102_20130103_AS_MILEAGE_CALC_N_20190806_08_20_33_v1.xml
      128 data-oasis-as-mileage-calc-all/xml/20130103_20130104_AS_MILEAGE_CALC_N_20190806_08_20_38_v1.xml

    You can see that the --include directive is ignored by 'sync'.

    However, the 'copy' command honors the --include directive.
    Since 'copy' is non-destructive, we'll use that.
"""


def copy(resource_name, source_dir, target="eap:eap"):
    """
    Copy source_dir to target space:bucket. Because DO limits a given space
    to 100 buckets/directories, it's simpler to just copy the entire 
    energy-dashboard project into a single bucket. Sigh. I don't like doing 
    this b/c it means the blast radius is _huge_ if I screw something up.

    """
    return do_command("rclone copy -v --include='*.zip' %s %s" % (source_dir, target))

def do_command(resource_name, cmd):
    logging.info({
        "src":"all", 
        "action":"rclone",
        "cmd":cmd
        })
    return os.system(cmd)
