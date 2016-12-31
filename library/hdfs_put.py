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


# TRICK:
# To debug exchange, on the namenode:
# ngrep -q -d eth0 -W normal  port 50070

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
        When a file is copied, target modification time is adjusted to the source value.
    required: true
    default: null
    aliases: []
  hdfs_dest:
    description:
      - HDFS absolute path where the file should be copied to.  
        If it is a directory, file will be copied into with its source name. 
        If not, this will be the target full path. In this case, dirname must exist
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
      - the default is C(yes), which will replace the remote file when size or modification time is different from the source. 
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

from xml.dom import minidom

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

    def setModificationTime(self, hdfsPath, modTime):
        url = "http://{0}/webhdfs/v1{1}?{2}op=SETTIMES&modificationtime={3}".format(self.endpoint, hdfsPath, self.auth, long(modTime)*1000)
        self.put(url)

    def putFileToHdfs(self, localPath, hdfsPath):
        url = "http://{0}/webhdfs/v1{1}?{2}op=CREATE&overwrite=true".format(self.endpoint, hdfsPath, self.auth)
        resp = requests.put(url, allow_redirects=False)
        if not resp.status_code == 307:
            error("Invalid returned http code '{0}' when calling '{1}'".format(resp.status_code, url))
        url2 = resp.headers['location']    
        f = open(localPath, "rb")
        resp2 = requests.put(url2, data=f, headers={'content-type': 'application/octet-stream'})
        if not resp2.status_code == 201:
           error("Invalid returned http code '{0}' when calling '{1}'".format(resp2.status_code, url2))
           
           
    
    

def error(message, *args):
    x = "" + message.format(*args)
    module.fail_json(msg = x)    

class Parameters:
    pass
                
                
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
                webHDFS= WebHDFS(endpoint, p.hdfs_user)
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
            webHDFS= WebHDFS(endpoint, p.hdfs_user)
            (x, err) = webHDFS.test()
            if x:
                p.webhdfsEndpoint = webHDFS.endpoint
                return webHDFS
            else:
                errors.append(err)
        error("Unable to find a valid 'webhdfs_endpoint' in: " + p.webhdfsEndpoint + " (" + str(errors) + ")")
    

def checkParameters(p):
    if not os.path.exists(p.src):
        module.fail_json(msg="Source %s not found" % (p.src))
    if not os.access(p.src, os.R_OK):
        module.fail_json(msg="Source %s not readable" % (p.src))
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

    if not p.hdfs_dest.startswith("/"):
        error("hdfs_dest '{0}' is not absolute. Absolute path is required!", p.path)

def applyAttrOnNewFile(webhdfs, path, p):
    owner = p.default_owner if p.owner is None else p.owner
    group = p.default_group if p.group is None else p.group
    mode = p.default_mode if p.mode is None else p.mode
    if owner != None:
        webhdfs.setOwner(path, owner)
    if group != None:
        webhdfs.setGroup(path, group)
    if mode != None:
        webhdfs.setPermission(path, mode)


def checkAndAdjustAttrOnExistingFile(webhdfs, fileStatus, p):
    if p.owner != None and p.owner != fileStatus['owner']:
        p.changed = True
        if not p.check_mode: 
            webhdfs.setOwner(p.hdfs_dest, p.owner)
    if p.group != None and p.group != fileStatus['group']:
        p.changed = True
        if not p.check_mode: 
            webhdfs.setGroup(p.hdfs_dest, p.group)
    if(p.mode != None and fileStatus['permission'] != p.mode):
        p.changed = True
        if not p.check_mode: 
            webhdfs.setPermission(p.hdfs_dest, p.mode)


                
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
    p.backup = module.params['backup']
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

    checkParameters(p)
    
    webhdfs = lookupWebHdfs(p)
    
    destStatus = webhdfs.getFileStatus(p.hdfs_dest)
    
    #print(destStatus)
            
    if not os.path.isdir(p.src):
        # Source is a simple file
        if destStatus != None and destStatus['type'] == 'DIRECTORY':
            # Target is a directory. Recompute effective target
            p.hdfs_dest = os.path.join(p.hdfs_dest, os.path.basename(p.src))
            destStatus = webhdfs.getFileStatus(p.hdfs_dest)
        if destStatus == None:
            # hdfs_dest does not exist. Ensure base dir exists
            destBasedir = os.path.dirname(p.hdfs_dest)
            destBaseDirStatus = webhdfs.getFileStatus(destBasedir)
            if destBaseDirStatus == None or destBaseDirStatus['type'] != 'DIRECTORY':
                error("Destination directory {0} does not exist", destBasedir)
            p.changed = True
            if not p.check_mode:
                webhdfs.putFileToHdfs(p.src, p.hdfs_dest)
                webhdfs.setModificationTime(p.hdfs_dest, int(os.stat(p.src).st_mtime))
                applyAttrOnNewFile(webhdfs, p.hdfs_dest, p)
        elif destStatus['type'] == 'FILE':
            stat = os.stat(p.src)
            if p.force and (stat.st_size != destStatus['length'] or  int(stat.st_mtime) != destStatus['modificationTime']/1000):
                #print("{{ statst_size: {0}, destStatus_length: {1}, int_stat_st_mtime: {2}, estStatus_modificationTime_1000: {3} }}".format(stat.st_size, destStatus['length'], int(stat.st_mtime), destStatus['modificationTime']/100))
                # File changed. Must be copied again
                p.changed = True
                if not p.check_mode:
                    if p.backup:
                        backupHdfsFile(webhdfs, p.hdfs_dest)
                    webhdfs.putFileToHdfs(p.src, p.hdfs_dest)
                    webhdfs.setModificationTime(p.hdfs_dest, int(stat.st_mtime))
                    applyAttrOnNewFile(webhdfs, p.hdfs_dest, p)
            else:
                checkAndAdjustAttrOnExistingFile(webhdfs, destStatus, p)
        elif destStatus['type'] == 'DIRECTORY':
            error("hdfs_dest '{0}' is a directory. Must be a file or not existing", p.hdfs_dest)
        else:
            error("Unknown type '{0}' for hdfs_dest '{1}'", destStatus['type'], p.hdfs_dest)
    else:
        # Handle source is folder case
        error("Source is folder: Not yet implemented")



    module.exit_json(changed=p.changed)
    

from ansible.module_utils.basic import *

if __name__ == '__main__':
    main()

