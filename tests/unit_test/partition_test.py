from dbt.adapters.odps.relation_configs import PartitionConfig

def test_partition_config():
    config = PartitionConfig.parse(
        [
            {"field": "ds", "data_type": "string"}
        ]
    )
    assert "ds string" == (config.render())