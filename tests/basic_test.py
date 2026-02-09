def test_package_import_and_version():
    import p_plp

    assert hasattr(p_plp, "__version__")
    assert isinstance(p_plp.__version__, str)