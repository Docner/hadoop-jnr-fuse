# hadoop-jnr-fuse

Fuse adapter to connect to Hadoop HDFS.

## Building

Netbeans project using ant. 

However, it needs both the native and the java code from com.github.jnr.jffi. 
I cloned:

git@github.com:jnr/jffi.git

Then, using an old JDK do: 

JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk /home/wiebe/netbeans-11.2/netbeans/extide/ant/bin/ant clean jar

Jdk > 10 does not compile headers and the javac native header compilation compiles only one out of 3 headers. 

This creates 3 jars in the dist subdirectory:
- jffi.jar which contains the JNR java code
- jffi-x86_64-Linux.jar which contains the linux / X64 native side
- jffi-complete.jar which contains both plus archived native libs for other platforms.

I manually took the jffi-complete.jar and replaces the jffi.jar that I got from the normal ivy retrieve on the maven repo.

The jnr-ffi jar and also the jnr-fuse jar are taken from their respective repositories. See ivy.xml.
However, you need the following repo's in your ivysettings: 

'''
<ibiblio name="bintray" root="https://jcenter.bintray.com"
                     m2compatible="true"/>
<ibiblio name="Sonatype OSS" m2compatible="true"
         root="https://oss.sonatype.org/content/repositories/snapshots"/>
 '''
