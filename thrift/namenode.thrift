include "shared.thrift"

namespace go tdfs

service NameNode {
    shared.Response RegisterDeprecated (
        1:map<string,shared.Metadata> meta_map;
        2:string client_ip
    )

    void Register (
        1:set<string> chunks;
        2:string datanode_ip;
    )

    list<string> GetSpareNodes ()

    list<string> GetDataNodesWithFile (
        1:required string file_path;
    )

    void UpdateMetadata (
        1:required string file_path;
        2:required shared.Metadata metadata;
    )

    shared.Response Put (
        1:required string path;
        2:required shared.Metadata metadata;
        3:required string client_ip;
    )

    void Delete (
        1:required string remote_file_path;
    )

    shared.FileStat Stat (
        1:required string remote_file_path;
    )

    map<string, shared.DNStat> ListDataNode()

    void Mkdir (
        1:required string remote_file_path;
    )

    void Rename (
        1:required string rename_src_path;
        2:required string rename_dest_path;
    )

    map<string, shared.FileStat> List(
        1:required string remote_dir_path;
    )

    string InitializePut (
        1:required string file_path;
        2:required shared.Metadata metadata;
        3:required i64 total_block;
    )

    shared.PutChunkResp PutChunk (
        1:required string task_id;
        2:required i64 seq;
        3:required string chunk_id;
    )

    shared.ChunkList GetChunkList (
        1:required string path;
        2:required i64 offset;
        3:required i64 size;
    )
}