#!/usr/bin/python
# -*- coding: utf-8 -*-

# (c) 2012, Michael DeHaan <michael.dehaan@gmail.com>, and others
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
#
# =========================================================================
# Modified by Serge ALEXANDRE for HDFS creates/removes relocation in HDFS.
# HDFS modification (c) 2015 BROADSoftware
#
#
#



import datetime
import shlex
import os

DOCUMENTATION = '''
---
module: hdfs_cmd
short_description: Executes a command on a remote node
description:
     - THIS IS A MODIFIED VERSION OF THE M(command) MODULE which lookup C(creates) and C(removes) files in HDFS instead of local file.
     - The M(hdfs_cmd) module takes a C(cmd) option holding all the command line to be executed 
     - The given command will be executed on all selected nodes. By default it will not be
       processed through the shell, so variables like C($HOME) and operations
       like C("<"), C(">"), C("|"), and C("&") will not work (Set uses_shell=true to activate theses features).
options:
  cmd:
    description:
      - the command to execute.
    required: true
    default: null
  hdfs_creates:
    description:
      - an absolute HDFS path, when it already exists, this step will B(not) be run.
    required: no
    default: null
  hdfs_removes:
    description:
      - an absolute HDFS path, when it does not exist, this step will B(not) be run.
    version_added: "0.8"
    required: no
    default: null
  uses_shell:
    description:
      - Activate shell mode. Same as C(shell) module against C(command) module.
  chdir:
    description:
      - cd into this directory before running the command
    version_added: "0.6"
    required: false
    default: null
  executable:
    description:
      - change the shell used to execute the command. Should be an absolute path to the executable.
    required: false
    default: null
    version_added: "0.9"
  hadoop_conf_dir:
    description:
      - Where to find Haddop configuration file, specially hdfs-site.xml, 
        in order to lookup WebHDFS endpoint (C(dfs.namenode.http-address))
        Used only if webhdfs_endpoint is not defined
    required: false
    default: "/etc/hadoop/conf"
  webhdfs_endpoint:
    description:
      - Provide WebHDFS REST API entry point. Typically C(<namenodeHost>:50070). 
        If not defined, will be looked up in local hdfs-site.xml
    required: false
    default: None
  auth:
    description:
      - Define account to impersonate to perform required operation. Technically, 
        this value will be inserted between the path and the C(op=XXXXX) value of 
        the built URL. Refer to C(https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Authentication)
        for more information.
    required: false
    default: "user.name=hdfs"
notes:
    -  If you want to run a command through the shell (say you are using C(<),
       C(>), C(|), etc), you actually need to set uses_shell=true. The
       M(command) module is much more secure as it's not affected by the user's
       environment.
    -  " C(creates), C(removes), and C(chdir) can be specified after the command. For instance, if you only want to run a command if a certain file does not exist, use this."
    - As HDFS is a distributed file system shared by all nodes of a cluster, 
      this module must be launched on one node only. Note there is no 
      protection against race condition (Same operation performed simultaneously
      from several nodes).
    - All HDFS operations are performed using WebHDFS REST API. 
author: 
    - Ansible Core Team
    - Michael DeHaan
    - Serge ALEXANDRE
    
    
'''

EXAMPLES = '''

# How to copy a file from the file system of the targeted host to HDFS
- hdfs_cmd: cmd="sudo -u joe hdfs dfs -put /etc/passwd /user/joe/passwd1" hdfs_creates=/user/joe/passwd1

# Same, using different syntax
- hdfs_cmd: cmd="sudo -u joe hdfs dfs -put /etc/passwd /user/joe/passwd2"
  args:
    hdfs_creates: /user/joe/passwd2
    
# Same, using different syntax
- name: "Copy passwd3 to hdfs"
  hdfs_cmd: 
    cmd: sudo -u joe hdfs dfs -put ./passwd /user/joe/passwd3
    hdfs_creates: /user/joe/passwd3
    chdir: /etc

# Copy the file and adjust permissions using hdfs_file
- hdfs_cmd: cmd="sudo -u hdfs hdfs dfs -put /etc/passwd /user/joe/passwd4" hdfs_creates=/user/joe/passwd4
- hdfs_file: hdfs_path=/user/joe/passwd4 owner=joe group=users mode=0770
      
'''


# -------------------------------------------------------------HDFS ADD ON
from xml.dom import minidom
from distutils.version import LooseVersion
HAS_HTTPLIB2 = False
# Version check was performed in ansible uri module. So, perform it for safety
try:
    import httplib2
    if LooseVersion(httplib2.__version__) >= LooseVersion('0.7'):
        HAS_HTTPLIB2 = True
except ImportError, AttributeError:
    # AttributeError if __version__ is not present
    pass


# Global, to allow access from error
module = None

class Parameters:
    changed = False


def error(message, *args):
    x = "" + message.format(*args)
    module.fail_json(msg = x)    

class WebHDFS:
    
    def __init__(self, endpoint, auth):
        if auth != "" and not auth.endswith("&"):
            auth = auth + "&"
        self.endpoint = endpoint
        self.auth = auth
            
    def test(self):
        url = "http://{0}/webhdfs/v1/?{1}op=GETFILESTATUS".format(self.endpoint, self.auth)
        try:
            h = httplib2.Http()
            resp, _ = h.request(url, "GET")
            if resp.status == 200:
                return (True, "")
            else: 
                return (False, "{0}  =>  Response code: {1}".format(url, resp.status))
        except Exception as e:
            return (False, "{0}  =>  Response code: {1}".format(url, e.strerror))
            
        
    class FileStatus:
        owner = None
        group  = None
        type = None
        permission = None
        def __str__(self):
            return "FileStatus => owner: '{0}', group: '{1}',  type:'{2}', permission:'{3}'".format(self.owner, self.group, self.type, self.permission)
        
    def getFileStatus(self, path):
        url = "http://{0}/webhdfs/v1{1}?{2}op=GETFILESTATUS".format(self.endpoint, path, self.auth)
        h = httplib2.Http()
        resp, content = h.request(url, "GET")
        if resp.status == 200:
            #print content
            result = json.loads(content)
            fileStatus = WebHDFS.FileStatus()
            fileStatus.owner = result['FileStatus']['owner']
            fileStatus.group = result['FileStatus']['group']
            fileStatus.permission = result['FileStatus']['permission']
            fileStatus.type = result['FileStatus']['type']
            return fileStatus
        elif resp.status == 404:
            return None
        else:
            error("Invalid returned http code '{0}' when calling '{1}'",resp.status, url)
            
            
            
             
                
def lookupWebHdfs(p):                
    if p.webhdfsEndpoint == None:
        candidates = []
        hspath = os.path.join(p.hadoopConfDir, "hdfs-site.xml")
        NN_HTTP_TOKEN1 = "dfs.namenode.http-address"
        NN_HTTP_TOKEN2 = "dfs.http.address"  # Deprecated
        if os.path.isfile(hspath):
            doc = minidom.parse(hspath)
            properties = doc.getElementsByTagName("property")
            for prop in properties :
                name = prop.getElementsByTagName("name")[0].childNodes[0].data
                if name.startswith(NN_HTTP_TOKEN1) or name.startswith(NN_HTTP_TOKEN2):
                    candidates.append(prop.getElementsByTagName("value")[0].childNodes[0].data)
            if not candidates:
                error("Unable to find {0}* or {1}* in {2}. Provide explicit 'webhdfs_endpoint'", NN_HTTP_TOKEN1, NN_HTTP_TOKEN2, hspath)
            errors = []
            for endpoint in candidates:
                webHDFS= WebHDFS(endpoint, p.auth)
                (x, err) = webHDFS.test()
                if x:
                    p.webhdfsEndpoint = webHDFS.endpoint
                    return webHDFS
                else:
                    errors.append("\n" + err)
            error("Unable to find a valid 'webhdfs_endpoint' in hdfs-site.xml:" + err)
        else:
            error("Unable to find file {0}. Provide 'webhdfs_endpoint' or 'hadoop_conf_dir' parameter", hspath)
    else:
        return WebHDFS(p.webhdfsEndpoint, p.auth)
    
                            
# ------------------------------------------------------------- end of HDFS ADD ON
            

def main():
    global module       # HDFS ADD ON

    module = AnsibleModule(
        argument_spec=dict(
            cmd = dict(),
            uses_shell = dict(type='bool', default=False),
            chdir = dict(),
            executable = dict(),
            hdfs_creates = dict(),
            hdfs_removes = dict(),
            # -------------- HDFS ADD ON
            hadoop_conf_dir = dict(required=False, default="/etc/hadoop/conf"), 
            webhdfs_endpoint = dict(required=False, default=None),
            auth = dict(required=False, default="user.name=hdfs")
            # -------------- End of HDFS ADD ON
        )
    )

    shell = module.params['uses_shell']
    chdir = module.params['chdir']
    executable = module.params['executable']
    cmd  = module.params['cmd']
    hdfs_creates  = module.params['hdfs_creates']
    hdfs_removes  = module.params['hdfs_removes']

    if cmd.strip() == '':
        module.fail_json(rc=256, msg="no command given")

    if chdir:
        chdir = os.path.abspath(os.path.expanduser(chdir))
        os.chdir(chdir)

    # -------------------------------------------------------------------------- HDFS ADD ON
    
        
    p = Parameters()
    p.hadoopConfDir = module.params['hadoop_conf_dir']
    p.webhdfsEndpoint = module.params['webhdfs_endpoint']
    p.auth = module.params['auth']
   
    webhdfs = lookupWebHdfs(p)
    
    if hdfs_creates:
        # do not run the command if the line contains creates=filename
        # and the filename already exists ON HDFS.  This allows idempotence
        # of command executions.
        fileStatus = webhdfs.getFileStatus(hdfs_creates)
        if fileStatus != None:
            module.exit_json(
                cmd=cmd,
                stdout="skipped, since %s exists on HDFS" % hdfs_creates,
                changed=False,
                stderr=False,
                rc=0
            )

    if hdfs_removes:
        # do not run the command if the line contains removes=filename
        # and the filename does not exist.  This allows idempotence
        # of command executions.
        fileStatus = webhdfs.getFileStatus(hdfs_removes)
        if fileStatus == None:
            module.exit_json(
                cmd=cmd,
                stdout="skipped, since %s does not exist on HDFS" % hdfs_removes,
                changed=False,
                stderr=False,
                rc=0
            )
    # -------------------------------------------------------------------- End of HDFS ADD ON


    if not shell:
        cmd = shlex.split(cmd)
    startd = datetime.datetime.now()

    rc, out, err = module.run_command(cmd, executable=executable, use_unsafe_shell=shell)

    endd = datetime.datetime.now()
    delta = endd - startd

    if out is None:
        out = ''
    if err is None:
        err = ''

    module.exit_json(
        cmd      = cmd,
        stdout   = out.rstrip("\r\n"),
        stderr   = err.rstrip("\r\n"),
        rc       = rc,
        start    = str(startd),
        end      = str(endd),
        delta    = str(delta),
        changed  = True
    )

# import module snippets
from ansible.module_utils.basic import *
from ansible.module_utils.splitter import *

main()
