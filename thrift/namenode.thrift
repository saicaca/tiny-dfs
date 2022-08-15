include "shared.thrift"

namespace go tdfs

service NameNode {
    shared.Response Put (
        1:required string local_file_path;
        2:required string remote_file_path;
    )

    shared.Response Get (
        1:required string remote_file_path;
        2:required string local_file_path;
    )

    shared.Response Delete (
        1:required string remote_file_path;
    )

    shared.Response Stat (
        1:required string remote_file_path;
    )

    shared.Response Mkdir (
        1:required string remote_file_path;
    )

    shared.Response Rename (
        1:required string rename_src_path;
        2:required string rename_dest_path;
    )

    shared.Response List(
        1:required string remote_dir_path;
    )
}