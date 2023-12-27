import abc
import json
import os


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Save state to local storage."""

        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Retrieve state from local storage."""

        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: str | None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w+') as json_file:
            json.dump(state, json_file)

    def retrieve_state(self) -> dict:
        json_object = {}
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r+') as json_file:
                try:
                    json_object = json.load(json_file)
                except json.JSONDecodeError:
                    pass
        else:
            print('xd')
            print(self.file_path)
            raise ValueError
        return json_object


class State:

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: any) -> None:
        """Set state for a key in storage."""

        data = self.storage.retrieve_state()
        data.update({key: value})
        self.storage.save_state(data)

    def get_state(self, key: str) -> any:
        """Get state by the key."""

        data = self.storage.retrieve_state()
        return data.get(key, None)


storage = JsonFileStorage('state/state.json')
state = State(storage)
