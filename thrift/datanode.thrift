include "shared.thrift"

namespace go tdfs

service DataNode {
    shared.Response Ping ()

    shared.Response Put (
        1:required string remote_file_path;
        2:required binary file_data;
        3:required shared.Metadata metadata;
    )

    shared.Response Get (
        1:required string remote_file_path;
    )

    shared.Response Delete (
        1:required string remote_file_path;
    )
}