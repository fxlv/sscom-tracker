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


    def test_construction_with_custom_settings_file_name(self, chdir, load_settings):
        s = load_settings
        assert isinstance(s, lib.settings.Settings)


    def test_tracking_list(self, chdir, load_settings):
        """Ensure that tracking list is interpreted correctly.

        Tracking list should be dictionary->list->dictionary.
        Each tracking entry should also specify the retrieval type.
        """
        s = load_settings
        assert isinstance(
            s.tracking_list, dict
        )  # dictionary of 'tracking item type': list
        assert isinstance(s.tracking_list["apartment"], list)  # list of dictionaries
        assert isinstance(
            s.tracking_list["apartment"][0]["type"], str
        )  # type is either 'rss' or 'scrape'
