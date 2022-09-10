include "shared.thrift"

namespace go tdfs

service DataNode {
    shared.DNStat Ping ()

    shared.Response Put (
        1:required string remote_file_path;
        2:required binary file_data;
        3:required shared.Metadata metadata;
    )

    shared.Response Get (
        1:required string remote_file_path;
    )

    void UpdateMetadata(
        1:required string path;
        2:required shared.Metadata metadata;
    )

    void MoveFile(
        1:required string old_path;
        2:required string new_path;
        3:required i64 request_time;
    )

    shared.Response Delete (
        1:required string remote_file_path;
    )

    shared.Response MakeReplica (
        1:required string target_addr;
        2:required string file_path;
    )

    shared.Response ReceiveReplica (
        1:required string file_path;
        2:required shared.File file;
    )

    shared.PutChunkResp PutChunk (
        1:required string task_id;
        2:required i64 offset;
        3:required binary data;
        4:required string md5;
    )
}