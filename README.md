# hdfs_modules

This ansible role host a set of modules aimed to manipulate file and directory on HDFS (The Hadoop Distributed File System).

* hdfs\_file: Equivalent of the ansible files/file module, but on HDFS. Doc [at this location](docs/hdfs_file.txt)

* hdfs\_info: Equivalent of the ansible files/info module, but on HDFS. Doc [at this location](docs/hdfs_info.txt)

* hdfs\_cmd: Equivalent of the ansible commands/command module, but on HDFS. Doc [at this location](docs/hdfs_cmd.txt)

## Requirements

These modules need the python-requests package to be present on the remote node.

# Example Playbook


	- hosts: edge_node1
	  roles:
	  - hdfs_modules
	  tasks:
	  # Create a directory if it does not exist.
	  # If already existing, adjust owner, group and mode if different.
	  - hdfs_file: hdfs_path=/user/joe/some_directory owner=joe group=users mode=0755 state=directory
	  # How to copy a file from the file system of the targeted host to HDF S
      - hdfs_cmd: cmd="sudo -u joe hdfs dfs -put /etc/passwd /user/joe/passwd1" hdfs_creates=/user/joe/passwd1 uses_shell=True
      #   And ajust owner, group and mode on the file
	  - hdfs_file: hdfs_path=/user/joe/passwd1 owner=joe group=users mode=0644
	

# License

GNU GPL

Click on the [Link](COPYING) to see the full text.
