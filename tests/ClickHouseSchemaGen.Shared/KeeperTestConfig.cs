namespace ClickHouseSchemaGen.Shared;

/// <summary>
/// Single-node ClickHouse Keeper config for Testcontainers cluster tests (not read from docker/).
/// </summary>
public static class KeeperTestConfig
{
    public static string Xml { get; } = """
        <clickhouse>
            <logger>
                <level>warning</level>
                <console>1</console>
            </logger>
            <listen_host>0.0.0.0</listen_host>
            <keeper_server>
                <tcp_port>9181</tcp_port>
                <server_id>1</server_id>
                <log_storage_path>/var/lib/clickhouse-keeper/coordination/log</log_storage_path>
                <snapshot_storage_path>/var/lib/clickhouse-keeper/coordination/snapshots</snapshot_storage_path>
                <coordination_settings>
                    <operation_timeout_ms>10000</operation_timeout_ms>
                    <session_timeout_ms>30000</session_timeout_ms>
                    <raft_logs_level>warning</raft_logs_level>
                </coordination_settings>
                <raft_configuration>
                    <server>
                        <id>1</id>
                        <hostname>clickhouse-keeper</hostname>
                        <port>9234</port>
                    </server>
                </raft_configuration>
            </keeper_server>
        </clickhouse>
        """;

    public static byte[] Utf8Bytes { get; } = Encoding.UTF8.GetBytes(Xml);
}
