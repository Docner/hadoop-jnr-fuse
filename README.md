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
Then, after building published it in ivy:
- ivy-jffi.xml in Development/java/external
- java -jar ~/ivy/home/ivy-jsch.jar -settings ~/ivy/ivysettings.xml -m2compatible -ivy ./ivy-jffi.xml -publish smoke-status -publishpattern ./jffi/dist/[artifact].[ext] -revision 1.2.24 -status integration -overwrite

The jnr-ffi jar and also the jnr-fuse jar are taken from their respective repositories. See ivy.xml.
However, you need the following repo's in your ivysettings: 

'''
<ibiblio name="bintray" root="https://jcenter.bintray.com"
                     m2compatible="true"/>
<ibiblio name="Sonatype OSS" m2compatible="true"
         root="https://oss.sonatype.org/content/repositories/snapshots"/>
 '''

The jnr-ffi dependency at the moment does not build using the maven dependency as it cannot be found, even with these
repos configured. 

Resolved to clone the github repos and start building the parts from that. 
- jnr-ffi is built with source version target 1.8 on jdk13
- ivy-jnr-jffi.xml in Development/java/external
- java -jar ~/ivy/home/ivy-jsch.jar -settings ~/ivy/ivysettings.xml -m2compatible -ivy ./ivy-jnr-ffi.xml -publish smoke-status -publishpattern ./jnr-ffi/target/[artifact]-[revision]-SNAPSHOT.[ext] -revision 2.1.17 -status integration -overwrite

- jnr-unixsocket created:
- mvn package
- ivy-jnr-unixsocket in Development/java/external
- 



## See also

https://github.com/SerCeMan/jnr-fuse
https://github.com/jnr/jnr-ffi

more then one entry point into the jpackaged bundle: 
https://docs.oracle.com/en/java/javase/14/jpackage/support-application-features.html#GUID-324F3A7B-409A-426D-AFB1-E4540049D13E

## Notes

fusermount -uz /home/wiebe/Development/java/apps/hadoop-jnr-fuse/mnt/hdfs

# from the project root: 
cp dist/hadoop-jnr-fuse.jar dist/lib/ && \ 
    /usr/lib/jvm/java-14-openjdk/bin/jpackage -i dist/lib/ \
    --main-class com.docner.hadoop.fuse.mount.SingleMountService \
    --main-jar hadoop-jnr-fuse.jar \
    --linux-package-name hadoop-jnr-fuse \
    --add-launcher "hdfs-single-mountd"=launcher-single-service.properties \
    --add-launcher "hadoopmount"=launcher-hadoopmount.properties 

sudo rpm -e hadoop-jnr-fuse
sudo rpm -iv ./hadoop-jnr-fuse-1.0-1.x86_64.rpm

mount -t hdfs hdfs://hdfs226:8020/wiebe ./hdfs/ \
      -o rw,owner,debug, blocking,'context="system_u:object_r:removable_t"'
mount -t hdfs hdfs://hdfs226:8020/wiebe ../mnt/hdfs/ -o rw,login,debug,blocking,'context="system_u:object_r:removable_t"'
mount -t hdfs hdfs://hdfs226:8020/wiebe ../mnt/hdfs/ -o rw,login,blocking'context="system_u:object_r:removable_t"'
