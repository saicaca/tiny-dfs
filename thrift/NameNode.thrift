namespace go NameNode

struct Response {
    1:i32 status;
    2:string msg;
}

service NameNode {
    Response Put (
        1:required string local_file_path;
        2:required string remote_file_path;
    )

    Response Get (
        1:required string remote_file_path;
        2:required string local_file_path;
    )

    Response Delete (
        1:required string remote_file_path;
    )

    Response Stat (
        1:required string remote_file_path;
    )

    Response Mkdir (
        1:required string remote_file_path;
    )

    Response Rename (
        1:required string rename_src_path;
        2:required string rename_dest_path;
    )

    Response List(
        1:required string remote_dir_path;
    )
}