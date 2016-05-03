Discussion of the approach is in the final writeup.
This file will explain how to run the program.

make will compile, assuming you have the correct environmental variables.

ENVIRONMENT VARIABLES
--------------------------------------------------------------------------------
setenv JAVA_HOME "/usr/lib/jvm/java/"
setenv HADOOP_HOME /u/cs258/hadoop
setenv HDFS_JAVA /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-2.b14.fc22.x86_64/jre/lib/amd64/server
setenv HDFS_HOME ${HADOOP_HOME}/lib/native

setenv PATH ${HADOOP_HOME}/bin:${PATH}
setenv CLASSPATH "`hadoop classpath --glob`"

# Set the default home directories for MPI and Cilk
setenv MPI_HOME /u/cs258/installed/mpi/3.0.2/x86_64

# set MPI binary and library paths
setenv PATH ${MPI_HOME}/bin:${PATH}

setenv LD_LIBRARY_PATH ${MPI_HOME}/lib:${HDFS_JAVA}:${HDFS_HOME}
--------------------------------------------------------------------------------



Alright, next you need to make sure that cycle1, cycle2, cycle3, and node2x14a
can all connect pairwise via password-less ssh.

The way to do this, since these machines all share the same file system,
is to go to do the following for each machine:

1. Log into the machine remotely
2. Do ssh-keygen -t rsa, and save the file into ~/.ssh/<machine_name>_id
3. Create a file ~/.ssh/config (if it doesn't exist) and add the following line:

Host cycle1 cycle1.csug.rochester.edu
    HostName cycle1.csug.rochester.edu
    IdentityFile ~/.ssh/cycle1_id
    User vliu5
    
(Change cycle1 to the machine you're using and vliu5 to your username, and the
identify file to whatever you saved it to)

4. Append the contents of <machine_file>_id.pub to ~/.ssh/authorized_keys


Now you should be able to type ssh cycle1, from cycle2 for example, and not
require a password. This will let MPI open connections properly.

If it doesn't work, check to see that ~/.ssh and its contents have the right
permissions (ideally 700 for all).



Finally, you may use the ./run script. WARNING: it hogs memory.









