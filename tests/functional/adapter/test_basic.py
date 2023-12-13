import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


class TestSimpleMaterializationsOdps(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def test_config(self):
        return {"require_full_refresh": True}


class TestSingularTestsOdps(BaseSingularTests):
    pass


class TestSingularTestsEphemeralOdps(BaseSingularTestsEphemeral):
    pass


class TestEmptyOdps(BaseEmpty):
    pass


class TestEphemeralOdps(BaseEphemeral):
    pass


class TestIncrementalOdps(BaseIncremental):
    pass


class TestGenericTestsOdps(BaseGenericTests):
    pass


class TestSnapshotCheckColsOdps(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampOdps(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodOdps(BaseAdapterMethod):
    pass
