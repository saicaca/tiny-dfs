include "shared.thrift"

namespace go tdfs

service DataNode {
    shared.DNStat Ping ()

    shared.Response Heartbeat ()

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
}