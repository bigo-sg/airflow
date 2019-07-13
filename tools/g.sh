gcc ../fuse-dfs/*.c -o fuse_dfs -D_FILE_OFFSET_BITS=64 -I .. -I ../libhdfs/include -I ../libhdfs \
-L /data/opt/hadoop-3.1.0/lib/native/ \
-Wl,-rpath=/data/opt/hadoop-3.1.0/lib/native/:/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server \
-lhdfs -lfuse -lpthread -lc -lm
