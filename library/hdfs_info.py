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
module: hdfs_info
version_added: "historical"
short_description: Grab information about an HDFS file or folder
description:
     - Allow testing of file/folder existence. And retrieve owner, group and mode of an existing file/folder on HDFS.
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
author: 
    - Serge ALEXANDRE
    
    
'''

EXAMPLES = '''



'''
RETURN = '''
hdfs_path:
    description: file/path to grab info from
    returned: always
    type: string
    sample: "/path/to/file.txt"
exits:
    description: If the path exists
    returned: always
    type: boolean
    sample: True
type:
    description: 'file', 'directory' or 'absent'
    returned: always
    type: string
    sample: "directory"
owner:
    description: Owner of the file or directory
    returned: if exists
    type: string
    sample: "joe"
group:
    description: Group of the file or directory
    returned: if exists
    type: string
    sample: "users"
mode:
    description: Permission of the file or directory
    returned: if exists
    type: string
    sample: "0755"
int_mode:
    description: Permission of the file or directory
    returned: if exists
    type: int
    sample: 493
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
            hdfs_path  = dict(required=True),
            hadoop_conf_dir = dict(required=False, default="/etc/hadoop/conf"),
            webhdfs_endpoint = dict(required=False, default=None),
            auth = dict(required=False, default="user.name=hdfs")
            
        )
    )
    
    if not HAS_HTTPLIB2:
        module.fail_json(msg="httplib2 >= 0.7 is not installed")    

    
    p = Parameters()
    p.path = module.params['hdfs_path']
    p.hadoopConfDir = module.params['hadoop_conf_dir']
    p.webhdfsEndpoint = module.params['webhdfs_endpoint']
    p.auth = module.params['auth']


    if not p.path.startswith("/"):
        error("Path '{0}' is not absolute. Absolute path is required!", p.path)
  
    webhdfs = lookupWebHdfs(p)
    
    fileStatus = webhdfs.getFileStatus(p.path)
    # NB: Need to set hdfs_path. If setting 'path', module.exit_json will add a 'state' referring to local file status.
    if fileStatus == None:
        module.exit_json(
            changed = False,
            hdfs_path = p.path,
            exists = False,
            type = "absent"
        )
    else:
        module.exit_json(
            changed = False,
            hdfs_path = p.path,
            exists = True,
            type = fileStatus.type.lower(),
            owner = fileStatus.owner,
            group = fileStatus.group,
            mode = "0" + fileStatus.permission,
            int_mode = int(fileStatus.permission, 8)
        )

    

from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()

