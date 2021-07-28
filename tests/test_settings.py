import lib.settings
import pytest
import os


class TestSettings:
    settings_file_name = "settings.test.json"
    settings_invalid_file_name = (
        "settings.test.invalid.json"  # file with one missing settings key
    )

    @pytest.fixture
    def load_settings(self):
        s = lib.settings.Settings(settings_file_name=self.settings_file_name)
        return s

    @pytest.fixture(scope="class")
    def chdir(self, request):
        print(f"dirname: {request.fspath.dirname}")
        print(f"invocation dir: {request.config.invocation_dir}")
        os.chdir(request.fspath.dirname)
        yield
        os.chdir(request.config.invocation_dir)

    def test_construction_fails_with_missing_settings_file(self, chdir):
        """When running tests, the default `settings.json` is missing.

        It should thrown an exception.
        """
        with pytest.raises(RuntimeError):
            s = lib.settings.Settings()

    def test_construction_with_custom_settings_file_name(self, chdir, load_settings):
        s = load_settings
        assert isinstance(s, lib.settings.Settings)

    def test_construction_with_an_invalid_settings_file(self, chdir):
        with pytest.raises(TypeError):
            s = lib.settings.Settings(
                settings_file_name=self.settings_invalid_file_name
            )
