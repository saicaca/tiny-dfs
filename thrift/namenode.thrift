include "shared.thrift"

namespace go tdfs

service NameNode {
    shared.Response Register (
        1:map<string,shared.Metadata> meta_map;
        2:string client_ip
    )

    shared.Response Put (
        1:required string path;
        2:required shared.Metadata metadata;
        3:required string client_ip
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