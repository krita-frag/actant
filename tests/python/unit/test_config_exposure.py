"""API 暴露面测试：_ActantConfig/_StoreConfig 字段透传、存储快照与 delete_workflow。"""

from __future__ import annotations

from pathlib import Path

import pytest

from actant.actant import _ActantConfig, _StoreConfig


class TestAdvancedConfigExposure:
    def test_store_fields_round_trip(self) -> None:
        config = _ActantConfig(
            payload_signing_key="",
            store=_StoreConfig(
                map_size=1 << 30,
                max_dbs=8,
                sync_mode="group_commit",
                flush_interval_ms=5,
            ),
        )
        assert config.store.map_size == 1 << 30
        assert config.store.max_dbs == 8
        assert config.store.sync_mode == "group_commit"
        assert config.store.flush_interval_ms == 5

    def test_worker_fields_round_trip(self) -> None:
        config = _ActantConfig(
            payload_signing_key="",
            prefetch_min=4,
            prefetch_max=32,
            worker_cancel_grace_ms=1500,
            pending_result_channel_capacity=64,
        )
        assert config.prefetch_min == 4
        assert config.prefetch_max == 32
        assert config.worker_cancel_grace_ms == 1500
        assert config.pending_result_channel_capacity == 64

    def test_workflow_fields_round_trip(self) -> None:
        config = _ActantConfig(
            payload_signing_key="",
            completed_retention_count=100,
            persist_flush_interval_ms=50,
            state_poll_interval_ms=100,
        )
        assert config.completed_retention_count == 100
        assert config.persist_flush_interval_ms == 50
        assert config.state_poll_interval_ms == 100

    def test_defaults_match_rust_side(self) -> None:
        config = _ActantConfig(payload_signing_key="")
        assert config.store.map_size == 32 * 1024 * 1024 * 1024
        assert config.store.max_dbs == 16
        assert config.store.sync_mode == "sync"
        assert config.prefetch_min == 16
        assert config.prefetch_max == 64
        assert config.worker_cancel_grace_ms == 2000
        assert config.pending_result_channel_capacity == 256
        assert config.completed_retention_count == 1000
        assert config.persist_flush_interval_ms == 200
        assert config.state_poll_interval_ms == 500

    def test_invalid_sync_mode_rejected_at_start(self) -> None:
        # 校验时机与 scheduler 一致：构造接受任意字符串，Runtime 启动
        # （config → Rust ActantConfig 转换）时以明确错误拒绝。
        import actant

        config = _ActantConfig(
            payload_signing_key="",
            store=_StoreConfig(sync_mode="eventual"),
        )
        rt = actant.Runtime(config=config)
        with pytest.raises(ValueError, match="sync_mode"):
            rt.start()


class TestStoreStats:
    def test_storage_stats_and_backup(self, tmp_path: Path) -> None:
        import actant

        rt = actant.Runtime.test()
        rt.start()
        try:
            stats = rt.storage_stats()
            assert stats.map_size == 32 * 1024 * 1024 * 1024
            assert stats.max_readers > 0
            assert stats.file_bytes > 0
            assert 0.0 <= stats.utilization_ratio <= 1.0

            backup = tmp_path / "backup.mdb"
            size = rt.backup_to(backup)
            assert backup.exists()
            assert size == backup.stat().st_size
        finally:
            rt.stop()

    def test_store_config_data_dir_is_used(self, tmp_path: Path) -> None:
        import actant

        target = tmp_path / "from-config"
        config = _ActantConfig(
            payload_signing_key="",
            store=_StoreConfig(data_dir=str(target)),
        )
        rt = actant.Runtime(config=config)
        assert rt._data_dir == str(target)


class TestDeleteWorkflow:
    def test_delete_unknown_workflow_is_noop(self) -> None:
        import actant

        rt = actant.Runtime.test()
        rt.start()
        try:
            # 幂等：删除不存在的工作流不应报错。
            rt.delete_workflow("wf-nonexistent")
        finally:
            rt.stop()
