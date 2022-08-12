namespace go DataNode

struct Response {
    1:i32 status;
    2:string msg;
}

service DataNode {
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
}