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


from xml.dom import minidom

DOCUMENTATION = '''
---
module: hdfs_put
version_added: "historical"
short_description: Copies files from remote locations up to HDFS
description:
     - The M(put) module copies a file or a folder from the remote box to HDFS. 
options:
  src:
    description:
      - Path on the remote box to a file to copy to HDFS. Can be absolute or relative.
        If path is a directory, it is copied recursively. In this case, if path ends
        with "/", only inside contents of that directory are copied to destination.
        Otherwise, if it does not end with "/", the directory itself with all contents
        is copied. This behavior is similar to Rsync.
    required: true
    default: null
    aliases: []
  hdfs_dest:
    description:
      - HDFS absolute path where the file should be copied to.  If it is a directory, file will be copied into with its source name. If not, this will be the target full path.
        If src is a directory, this must be a directory too.
    required: true
    default: null
  backup:
    description:
      - Create a backup file including the timestamp information so you can get
        the original file back if you somehow clobbered it incorrectly.
    required: false
    choices: [ "yes", "no" ]
    default: "no"
  force:
    description:
      - the default is C(yes), which will replace the remote file when contents are different than the source. 
        If C(no), the file will only be transferred if the destination does not exist.
    required: false
    choices: [ "yes", "no" ]
    default: "yes"
  directory_mode:
    description:
      - When doing a recursive copy set the mode for the directories. If this is not set we will use the system
        defaults. The mode is only set on directories which are newly created, and will not affect those that
        already existed.
    required: false
  follow:
    required: false
    default: "no"
    choices: [ "yes", "no" ]
    description:
      - 'This flag indicates that filesystem links, if they exist, should be followed.'
  owner:
    description:
      - Name of the user that will own the file, as would be fed by HDFS 'FileSystem.setOwner' 
    required: false
    default: None
  group:
    description:
      - Name of the group that will own the file, as would be fed by HDFS 'FileSystem.setOwner' 
    required: false
    default: None
  mode:
    description:
      - Mode (Permission) the file will be set, such as 0644 as would be fed by HDFS 'FileSystem.setPermission' 
    required: false
    default: None
  default_owner:
    description:
      - Name of the user that will own the file in case of creation. Existing file will not be modified.
    required: false
    default: None
  default_group:
    description:
      - Name of the group that will own the file in case of creation. Existing file will not be modified.
    required: false
    default: None
  default_mode:
    description:
      - Mode (Permission) the file will be set in case of creation. Existing file will not be modified.
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
        It could also be a comma separated list of entry point, which will be checked up to a valid one. This will allow Namenode H.A. handling. 
        If not defined, will be looked up in local hdfs-site.xml
    required: false
    default: None
  hdfs_user:
    description: Define account to impersonate to perform required operation on HDFS through WebHDFS.
    required: false
    default: "hdfs"
      
author:
    - "Serge ALEXANDRE"

'''


EXAMPLES = '''

'''



HAS_REQUESTS = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError, AttributeError:
    # AttributeError if __version__ is not present
    pass

# Global, to allow access from error
module = None

class WebHDFS:
    def __init__(self, endpoint, hdfsUser):
        self.endpoint = endpoint
        self.auth = "user.name=" + hdfsUser + "&"
            
    def test(self):
        url = "http://{0}/webhdfs/v1/?{1}op=GETFILESTATUS".format(self.endpoint, self.auth)
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                return (True, "")
            else: 
                return (False, "{0}  =>  Response code: {1}".format(url, resp.status_code))
        except Exception as e:
            return (False, "{0}  =>  Response code: {1}".format(url, str(e)))
        

    def getFileStatus(self, path):
        url = "http://{0}/webhdfs/v1{1}?{2}op=GETFILESTATUS".format(self.endpoint, path, self.auth)
        resp = requests.get(url)
        if resp.status_code == 200:
            #print content
            result =  resp.json()
            return result['FileStatus']
        elif resp.status_code == 404:
            return None
        else:
            error("Invalid returned http code '{0}' when calling '{1}'",resp.status_code, url)
            
    def put(self, url):
        resp = requests.put(url, allow_redirects=False)
        if resp.status_code != 200:  
            error("Invalid returned http code '{0}' when calling '{1}'", resp.status_code, url)

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
    

def error(message, *args):
    x = "" + message.format(*args)
    module.fail_json(msg = x)    

                
                
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
                webHDFS= WebHDFS(endpoint, p.hdfsUser)
                (x, err) = webHDFS.test()
                if x:
                    p.webhdfsEndpoint = webHDFS.endpoint
                    return webHDFS
                else:
                    errors.append(err)
            error("Unable to find a valid 'webhdfs_endpoint' in hdfs-site.xml:" + str(errors))
        else:
            error("Unable to find file {0}. Provide 'webhdfs_endpoint' or 'hadoop_conf_dir' parameter", hspath)
    else:
        candidates = p.webhdfsEndpoint.split(",")
        errors = []
        for endpoint in candidates:
            webHDFS= WebHDFS(endpoint, p.hdfsUser)
            (x, err) = webHDFS.test()
            if x:
                p.webhdfsEndpoint = webHDFS.endpoint
                return webHDFS
            else:
                errors.append(err)
        error("Unable to find a valid 'webhdfs_endpoint' in: " + p.webhdfsEndpoint + " (" + str(errors) + ")")
    
                
def main():
    
    global module
    module = AnsibleModule(
        argument_spec = dict(
            backup = dict(required=False, type='bool', default=False),
            default_group = dict(required=False, default=None),
            default_mode = dict(required=False, default=None),
            default_owner = dict(required=False, default=None),
            directory_mode = dict(required=False, default=None),
            follow = dict(required=False, type='bool', default=False),
            force = dict(required=False, type='bool', default=True),
            group = dict(required=False, default=None),
            hadoop_conf_dir = dict(required=False, default="/etc/hadoop/conf"),
            hdfs_dest  = dict(required=True),
            hdfs_user = dict(required=False, default="hdfs"),
            mode = dict(required=False, default=None),
            owner = dict(required=False, default=None),
            src  = dict(required=True, default=None),
            webhdfs_endpoint = dict(required=False, default=None),
        ),
        supports_check_mode=True
    )
    
    if not HAS_REQUESTS:
        module.fail_json(msg="python-requests module is not installed")    

    
    p = Parameters()
    p.backup = module.param['backup']
    p.default_group = module.params['default_group']
    p.default_mode = module.params['default_mode']
    p.default_owner = module.params['default_owner']
    p.directory_mode = module.params['directory_mode']
    p.follow = module.params['follow']
    p.force = module.params['force']
    p.group = module.params['group']
    p.hadoopConfDir = module.params['hadoop_conf_dir']
    p.hdfs_dest = module.params['hdfs_dest']
    p.hdfs_user = module.params['hdfs_user']
    p.mode = module.params['mode']
    p.owner = module.params['owner']
    p.src = module.params['src']
    p.webhdfsEndpoint = module.params['webhdfs_endpoint']

    p.check_mode = module.check_mode
    p.changed = False
    

    module.exit_json(changed=p.changed)
    

from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()

