#!/usr/bin/python
# -*- coding: utf-8 -*-

# (c) 2015, BROADSoftware
#
# This software is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software. If not, see <http://www.gnu.org/licenses/>.


from distutils.version import LooseVersion
from xml.dom import minidom

try:
    import json
except ImportError:
    import simplejson as json

HAS_HTTPLIB2 = False

DOCUMENTATION = '''
---
module: hdfs_file
short_description: Sets attributes of an HDFS file, or create HDFS directory
description:
     - Sets attributes of HDFS files, and directories, or removes them.
       Allow also HDFS directory creation.
     - Similar to Ansible File module, but operate on HDFS files. 
notes:
    - As HDFS is a distributed file system shared by all nodes of a cluster, 
      this module must be launched on one node only. Note there is no 
      protection against race condition (Same operation performed simultaneously
      from several nodes).
    - All HDFS operations are performed using WebHDFS REST API. 
requirements: [ ]
author: 
    - "Serge ALEXANDRE"
options:
  hdfs_path:
    description:
      - 'HDFS path to the file being managed.  Aliases: I(dest), I(name)'
    required: true
    default: None
  state:
    description:
      - If C(directory), all immediate sub-directories will be created if they
        do not exists, by calling HDFS FileSystem.mkdirs
        If C(file), the file will NOT be created if it does not exist.
        In both cases, owner, group and mode will be adjusted to provided value.
        If C(absent), directories will be recursively deleted (USE WITH CARE), 
        and file will be deleted.
    required: false
    default: None
    choices: [ file, directory, absent ]
  owner:
    description:
      - Name of the user that will own the file/directory, as would
        be fed by HDFS 'FileSystem.setOwner' 
    required: false
    default: None
  group:
    description:
      - Name of the group that will own the file/directory, as would
        be fed by HDFS 'FileSystem.setOwner' 
    required: false
    default: None
  mode:
    description:
      - Mode (Permission) the file or directory will be set, such as 0644 as would be
        fed by HDFS 'FileSystem.setPermission' 
    required: false
    default: None
  default_owner:
    description:
      - Name of the user that will own the directory in case of creation. Existing directory will not be modified.
    required: false
    default: None
  default_group:
    description:
      - Name of the group that will own the directory in case of creation. Existing directory will not be modified.
    required: false
    default: None
  default_mode:
    description:
      - Mode (Permission) the directory will be set in case of creation. Existing directory will not be modified.
    required: false
    default: None
  hadoop_conf_dir:
    description:
      - Where to find Hadoop configuration file, specially hdfs-site.xml, 
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
author: 
    - Serge ALEXANDRE
    
    
'''

EXAMPLES = '''

# Create a directory if it does not exist. 
# If already existing, adjust owner, group and mode if different.
- hdfs_file: hdfs_path=/user/joe/some_directory owner=joe group=users mode=0755 state=directory

# Remove this folder.
- hdfs_file: hdfs_path=/user/joe/some_directory state=absent

# Change permission. Only hdfs user will be able to access this file or folder.
- hdfs_file: hdfs_path=/user/hdfs/some_file_or_folder owner=hdfs group=hdfs mode=0700

# Change only permission on a file. Leave owner and group unchanged
- name: Change permission on this_file
  hdfs_file:
    hdfs_path: /usr/joe/some_file
    mode: 0600

# Ensure the directory exists. If yes, do not touch it. If no, create it with provided default_xxxx values.
- hdfs_file: hdfs_path=/user/joe/may_exist_directory default_owner=joe default_group=users default_mode=0755 state=directory


'''


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
            
    def put(self, url):
        h = httplib2.Http()
        (resp, _) = h.request(url, "PUT")
        if resp.status != 200:  
            error("Invalid returned http code '{0}' when calling '{1}'", resp.status, url)

    def createFolder(self, path, permission):
        if permission != None:
            url = "http://{0}/webhdfs/v1{1}?{2}op=MKDIRS&permission={3}".format(self.endpoint, path, self.auth, permission)
        else:
            url = "http://{0}/webhdfs/v1{1}?{2}op=MKDIRS".format(self.endpoint, path, self.auth)
        self.put(url)

    def setOwner(self, path, owner):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETOWNER&owner={3}".format(self.endpoint, path, self.auth, owner)
        self.put(url)

    def setGroup(self, path, group):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETOWNER&group={3}".format(self.endpoint, path, self.auth, group)
        self.put(url)
    
    def setPermission(self, path, permission):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETPERMISSION&permission={3}".format(self.endpoint, path, self.auth, permission)
        self.put(url)
    
    def delete(self, path):
        url = "http://{0}/webhdfs/v1{1}?{2}op=DELETE&recursive=true".format(self.endpoint, path, self.auth)
        h = httplib2.Http()
        (resp, _) = h.request(url, "DELETE")
        if resp.status != 200:  
            error("Invalid returned http code '{0}' when calling '{1}'", resp.status, url)
        
            
class State:
    FILE = "file"
    ABSENT = "absent"
    DIRECTORY = "directory"
    
class HdfsType:
    FILE = "FILE"
    DIRECTORY = "DIRECTORY"


def error(message, *args):
    x = "" + message.format(*args)
    module.fail_json(msg = x)    


class Parameters:
    changed = False


def checkAndAdjustAttributes(webhdfs, fileStatus, p):
    if p.owner != None and p.owner != fileStatus.owner:
        p.changed = True
        if not p.check_mode: 
            webhdfs.setOwner(p.path, p.owner)
    if p.group != None and p.group != fileStatus.group:
        p.changed = True
        if not p.check_mode: 
            webhdfs.setGroup(p.path, p.group)
    if(p.mode != None and fileStatus.permission != p.mode):
        p.changed = True
        if not p.check_mode: 
            webhdfs.setPermission(p.path, p.mode)


def checkCompletion(webhdfs, p):
    fs = webhdfs.getFileStatus(p.path)
    if fs == None:
        if p.state != State.ABSENT :
            error("Was unable to create {0}", p.path)
        else:
            pass    
    else:
        if p.state == State.ABSENT:
            error("Was unable to delete {0}", p.path)
        else:
            if p.owner != None and fs.owner != p.owner:
                error("Was unable to switch owner to {0}. Still {1}", p.owner, fs.owner) 
            if p.group != None and fs.group != p.group:
                error("Was unable to switch group to {0}. Still {1}", p.group, fs.group) 
            if p.mode != None and fs.permission != p.mode:
                error("Was unable to switch permission to {0}. Still {1}", p.mode, fs.permission) 
                
                
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
    
                
                
def main():
    
    global module
    module = AnsibleModule(
        argument_spec = dict(
            state = dict(required=False, choices=['file','directory','absent']),
            hdfs_path  = dict(required=True),
            owner = dict(required=False),
            group = dict(required=False),
            mode = dict(required=False),
            default_owner = dict(required=False),
            default_group = dict(required=False),
            default_mode = dict(required=False),
            hadoop_conf_dir = dict(required=False, default="/etc/hadoop/conf"),
            webhdfs_endpoint = dict(required=False, default=None),
            auth = dict(required=False, default="user.name=hdfs")
            
        ),
        supports_check_mode=True
    )
    
    if not HAS_HTTPLIB2:
        module.fail_json(msg="httplib2 >= 0.7 is not installed")    

    
    p = Parameters()
    p.state = module.params['state']
    p.path = module.params['hdfs_path']
    p.owner = module.params['owner']
    p.group = module.params['group']
    p.mode = module.params['mode']
    p.default_owner = module.params['default_owner']
    p.default_group = module.params['default_group']
    p.default_mode = module.params['default_mode']
    p.hadoopConfDir = module.params['hadoop_conf_dir']
    p.webhdfsEndpoint = module.params['webhdfs_endpoint']
    p.auth = module.params['auth']
    p.check_mode = module.check_mode

    if p.mode != None:
        if not isinstance(p.mode, int):
            try:
                p.mode = int(p.mode, 8)
            except Exception:
                error("mode must be in octal form")
    
        p.mode = oct(p.mode).lstrip("0")
        #print '{ mode_type: "' + str(type(p.mode)) + '",  mode_value: "' + str(p.mode) + '"}'

    if p.default_mode != None:
        if not isinstance(p.default_mode, int):
            try:
                p.default_mode = int(p.default_mode, 8)
            except Exception:
                error("default_mode must be in octal form")
    
        p.default_mode = oct(p.default_mode).lstrip("0")

    if(p.owner != None and p.default_owner != None):
        error("There is no reason to define both owner and default_owner")
    if(p.group != None and p.default_group != None):
        error("There is no reason to define both group and default_group")
    if(p.mode != None and p.default_mode != None):
        error("There is no reason to define both mode and default_mode")

    if not p.path.startswith("/"):
        error("Path '{0}' is not absolute. Absolute path is required!", p.path)

    webhdfs = lookupWebHdfs(p)
    
    fileStatus = webhdfs.getFileStatus(p.path)
    if fileStatus == None:
        if p.state == State.ABSENT:
            pass    # Fine. Nothing to do
        elif p.state != State.DIRECTORY:
            error("This module can only create Folder. State should be 'directory' and not '{0}'", p.state)
        else:
            p.changed = True
            if not p.check_mode:
                owner = p.default_owner if p.owner is None else p.owner
                group = p.default_group if p.group is None else p.group
                mode = p.default_mode if p.mode is None else p.mode
                webhdfs.createFolder(p.path, mode)
                if owner != None:
                    webhdfs.setOwner(p.path, owner)
                if group != None:
                    webhdfs.setGroup(p.path, group)
    else:
        if p.state == None:
            checkAndAdjustAttributes(webhdfs, fileStatus, p)
        elif p.state == State.ABSENT:
            p.changed = True
            if not p.check_mode:
                webhdfs.delete(p.path)
        elif p.state == State.FILE and fileStatus.type == HdfsType.DIRECTORY:
            error("Path '{0}' is a directory. Can't convert to a file", p.path)
        elif p.state == State.DIRECTORY and fileStatus.type == HdfsType.FILE:
            error("Path '{0}' is a file. Can't convert to a directory", p.path)
        elif p.state == State.FILE and fileStatus.type == HdfsType.FILE:
            checkAndAdjustAttributes(webhdfs, fileStatus, p)
        elif p.state == State.DIRECTORY and fileStatus.type == HdfsType.DIRECTORY:
            checkAndAdjustAttributes(webhdfs, fileStatus, p)
        else:
            error("State mismatch: Requested:{0}  HDFS:{1}", p.state, fileStatus.type)
    
    if not p.check_mode:
        checkCompletion(webhdfs, p)    
    
    module.exit_json(changed=p.changed)

from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()

