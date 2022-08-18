namespace go tdfs

struct Metadata {
    1: bool isDeleted,
    2: string name,
    3: i64 mtime,
    4: i64 size,
}

struct Response {
    1: i32 status;
    2: string msg;
    3: File file;
}

struct File {
    1: binary data;
    2: Metadata medatada;
}

struct DNStat {
    1: i64 fileNum;
    2: i64 usedSpace;
    3: i64 totalSpace;
    4: i64 traffic;
}

struct FileStat {
    1: bool isDir
    2: Metadata medatada;
    3: i32 replica;
}