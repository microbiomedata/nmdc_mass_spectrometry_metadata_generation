import nmdc_ms_metadata_gen.id_pool as id_pool_module
import nmdc_ms_metadata_gen.metadata_input_check as metadata_input_check_module
from nmdc_ms_metadata_gen.id_pool import IDPool
from nmdc_ms_metadata_gen.metadata_input_check import MetadataSurveyor


def test_metadata_surveyor_clients_use_api_base_url(monkeypatch):
    calls = []

    class DummyDataGenerationSearch:
        def __init__(self, **kwargs):
            calls.append(("dg", kwargs))

    class DummyBiosampleSearch:
        def __init__(self, **kwargs):
            calls.append(("bs", kwargs))

    monkeypatch.setattr(
        metadata_input_check_module, "DataGenerationSearch", DummyDataGenerationSearch
    )
    monkeypatch.setattr(
        metadata_input_check_module, "BiosampleSearch", DummyBiosampleSearch
    )

    MetadataSurveyor(study="nmdc:sty-11-test")

    assert calls == [
        ("dg", {"api_base_url": metadata_input_check_module.API_BASE_URL}),
        ("bs", {"api_base_url": metadata_input_check_module.API_BASE_URL}),
    ]


def test_id_pool_minter_uses_api_base_url(monkeypatch):
    calls = []

    class DummyMinter:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def mint(self, nmdc_type, count, client_id, client_secret):
            return [f"nmdc:dobj-00-{i:08d}" for i in range(count)]

    monkeypatch.setattr(id_pool_module, "Minter", DummyMinter)

    pool = IDPool(pool_size=2, refill_threshold=0, test=False)
    pool._refill_pool("nmdc:DataObject", client_id="id", client_secret="secret")

    assert calls == [{"api_base_url": id_pool_module.API_BASE_URL}]
